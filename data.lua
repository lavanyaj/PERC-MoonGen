local device	= require "device"
local ffi = require("ffi")
local dpdk	= require "dpdk"
local dpdkc	= require "dpdkc"
local log = require "log"
local memory	= require "memory"
local ntoh16, hton16 = ntoh16, hton16
local stats		= require "stats"
local pkt = require("packet")
local pipe		= require "pipe"

local fsd = require "examples.perc-moongen-single.flow-size-distribution"

local percg = require "proto.percg"
local percc1 = require "proto.percc1"
local eth = require "proto.ethernet"

local ipc = require "examples.perc-moongen-single.ipc"
local monitor = require "examples.perc-moongen-single.monitor"
local perc_constants = require "examples.perc-moongen-single.constants"

local DATA_PACKET_SIZE	= perc_constants.DATA_PACKET_SIZE
local ACK_PACKET_SIZE = perc_constants.ACK_PACKET_SIZE

ffi.cdef [[
typedef struct foo { bool active; 
uint64_t flow, size, sent, acked; double acked_time, start_time;
uint8_t percgSrc, percgDst;
union mac_address ethSrc, ethDst;}
 txQueueInfo;
typedef struct bar {
uint64_t flow, recv, acked, size;
uint8_t percgSrc, percgDst;
union mac_address ethSrc, ethDst;}
 rxQueueInfo;
]]

dataMod = {}

function dataMod.rxlog(str)
   if perc_constants.LOG_RXDATA then
      print("data2.lua rx log: " .. str)
   end
end

function dataMod.txlog(str)
   if perc_constants.LOG_TXDATA then
      print("data2.lua tx log: " .. str)
   end
end

function dataMod.warn(str)
   if perc_constants.WARN_DATA then
      print("data2.lua warn: " .. str)
   end
end

-- receives data packets and sends acks
function dataMod.rxSlave(dev, readyInfo)
   local thisCore = dpdk.getCore()
   dataMod.rxlog("Data rx slave running on dev "
		   .. dev.id .. ", core " .. thisCore
		   .. " dpdk running " .. tostring(dpdk.running()))

   
   ipc.waitTillReady(readyInfo)
   local mem = memory.createMemPool{
      ["func"]=function(buf) 
   	 buf:getPercgPacket():fill{
   	    pktLength = ACK_PACKET_SIZE,
   	    ethType = eth.TYPE_ACK,
   	    ethSrc = perc_constants.ACK_TXQUEUE}
      end,
      ["n"] = perc_constants.DATA_RECV_TXMEMPOOL_SIZE
   }
   local txBufs = mem:bufArray()
   --txBufs:alloc()
   local queueInfo = ffi.new("rxQueueInfo[?]",
   			     perc_constants.MAX_QUEUES+1, {}) -- indexing from 1
   -- invariants
   for q=1,perc_constants.MAX_QUEUES do
      queueInfo[q].recv = 0ULL
      queueInfo[q].size = 0ULL
      queueInfo[q].acked = 0ULL
   end
   
   local txQueue = dev:getTxQueue(perc_constants.ACK_TXQUEUE)
   local rxQueue = dev:getRxQueue(perc_constants.DATA_RXQUEUE)
   local rxCtr = stats:newDevRxCounter(rxQueue, "plain")
   local rxBufs = memory.bufArray()
   local seq = 0ULL
   local lastAckTime = dpdk.getTime()

   
   while dpdk.running() do
      --dataMod.rxlog("receive data packets\n")
      local ackNow = false      
      do
	 local rx = rxQueue:recv(rxBufs)
	 for b = 1, rx do
	    local buf = rxBufs[b]
	    local pkt = buf:getPercgPacket()
	    -- TODO(lav): exchange src and dst	    
	    -- assert(pkt.eth:getType() == eth.TYPE_PERC_DATA)
	    local flow = pkt.payload.uint64[0]
	    local q = pkt.payload.uint64[1]
	    -- local seqNo = pkt.payload.uint64[2]	
	    -- local checksumRx = pkt.payload.uint64[3]
	    local size = pkt.payload.uint64[4]
	    -- local checksumC = flow + q + seqNo
	    -- if (checksumRx ~= checksumC) then
	    --    dataMod.warn("checksum doesn't match recvd for data pkt "
	    -- 			.. pkt.percg:getString()
	    -- 			.. " flow " .. tostring(flow)
	    -- 			.. " q " .. tostring(q)
	    -- 			.. " seqNo " .. tostring(seqNo)
	    -- 			.. " checksumRx " .. tostring(checksumRx)
	    -- 			.. " computed " .. tostring(checksumC))
	    -- end
	    -- assert(checksumRx == checksumC)
	    if queueInfo[q].flow ~= flow then
	       queueInfo[q].flow = flow 
	       queueInfo[q].recv = 0ULL
	       queueInfo[q].acked = 0ULL
	       queueInfo[q].size = size
	       queueInfo[q].ethSrc = pkt.eth.src
	       queueInfo[q].ethDst = pkt.eth.dst
	       queueInfo[q].percgSrc = pkt.percg:getSource()
	       queueInfo[q].percgDst = pkt.percg:getDestination()
	    --    -- dataMod.rxlog("rx set up queue " .. q
	    --    -- 		      .. " for flow " .. tostring(flow)
	    --    -- 			 .. " recv " .. tostring(queueInfo[q].recv)
	    --    -- 			 .. " acked " .. tostring(queueInfo[q].acked)
	    --    -- 			 .. ", flow size " .. tostring(queueInfo[q].size))
	    end
	    -- assert(seqNo < size)
	    assert(queueInfo[q].size == size)	   
	    queueInfo[q].recv = queueInfo[q].recv + 1
	    if (queueInfo[q].recv == queueInfo[q].size) then
	       ackNow = true
	    end
	    -- if (queueInfo[q].recv >  queueInfo[q].size) then
	    --     dataMod.warn("rx received more packets than size"
	    --  		      .. " for flow " .. tostring(flow)
	    --  			 .. " size " .. tostring(queueInfo[q].size)
	    --  			 .. " < recv " .. tostring(queueInfo[q].recv))
	    -- end	 
	    assert(queueInfo[q].recv <= queueInfo[q].size)	    
	 end
	 if rx > 0 then
	    rxCtr:update()
	    rxBufs:freeAll()
	    end
      end      
      
      -- ACK when total size = recv or every rx_ack_timeout
      do
      	 local now = dpdk.getTime()
      	 if false and now > lastAckTime + perc_constants.rx_ack_timeout
      	 or ackNow then
      	    lastAckTime = now
      	    local newAcks = 0
      	    for q=1,perc_constants.MAX_QUEUES do
      	       assert(queueInfo[q].recv <= queueInfo[q].size)
      	       if queueInfo[q].recv > queueInfo[q].acked then
      		  newAcks = newAcks + 1 end
      	    end
	    if newAcks > 0 then
	       txBufs:allocN(ACK_PACKET_SIZE, newAcks)	    
	       local b = 1
	       for q=1,perc_constants.MAX_QUEUES do	 
		  if queueInfo[q].recv > queueInfo[q].acked then
		     queueInfo[q].acked = queueInfo[q].recv
      		  assert(b <= newAcks)
      		  local pkt = txBufs[b]:getPercgPacket()
      		  b = b + 1
      		  pkt.payload.uint64[0] = queueInfo[q].flow 
      		  pkt.payload.uint64[1] = q -- lua number -> double -> 32b
      		  pkt.payload.uint64[2] = queueInfo[q].acked
      		  pkt.payload.uint64[3] = queueInfo[q].flow
      		     + pkt.payload.uint64[1] + queueInfo[q].recv
      		  pkt.payload.uint64[4] = queueInfo[q].size
      		  pkt.eth:setType(eth.TYPE_ACK)
      		  pkt.eth:setSrc(perc_constants.ACK_TXQUEUE)
		  pkt.eth:setDst(queueInfo[q].ethSrc)
		  pkt.percg:setSource(queueInfo[q].percgDst)
		  pkt.percg:setDestination(queueInfo[q].percgSrc)
		  end
	       end
	       txQueue:send(txBufs)
	    end
      	 end -- ends if ackTime..	 
      end -- ends do

   end -- ends while dpdk.running
   rxCtr:finalize()
   dataMod.rxlog("dpdk running on rxslave " .. tostring(dpdk.running()))
end
   
-- sends data packets and receives acks
function dataMod.txSlave(dev, cdfFilepath, numFlows,
			 percgSrc, ethSrc,
			 tableDst, readyInfo)
   dpdk.sleepMillis(500)   
   local thisCore = dpdk.getCore()
   dataMod.txlog("Data tx slave running on dev "
		   .. dev.id .. ", core " .. thisCore)

   local flowSizes = fsd.create()
   flowSizes:loadCDF(cdfFilepath)
   local avgFlowSize = flowSizes:avg()
   assert(avgFlowSize > 0)
   log:info("loaded flow sizes file with avg. flow size "
     	      .. tostring(avgFlowSize/1500) .. " packets.\n")

   local percgDst, ethDst = next(tableDst)

   if type(ethSrc) == "number" then
      local buf = ffi.new("char[20]")
      dpdkc.get_mac_addr(ethSrc, buf)
      local ethSrcStr = ffi.string(buf)      
      ethSrc = parseMacAddress(ethSrcStr)
   elseif istype(macAddrType, ethSrc) then
      ethSrc = ethSrc
   else
      assert(false)
   end

   if type(ethDst) == "number" then
      local buf = ffi.new("char[20]")
      dpdkc.get_mac_addr(ethDst, buf)
      local ethDstStr = ffi.string(buf)      
      ethDst = parseMacAddress(ethDstStr)
   elseif istype(macAddrType, ethDst) then
      ethDst = ethDst
   else
      assert(false)
   end

   ipc.waitTillReady(readyInfo)

   local mem = {}
   local txBufs = {}

   local freeQueues = {}
   for i=1, perc_constants.MAX_QUEUES do
      if i ~= perc_constants.CONTROL_TXQUEUE
	 and i ~= perc_constants.ACK_TXQUEUE
      and i ~= perc_constants.DROP_QUEUE then 
	 table.insert(freeQueues, i)
      end
   end

   for q = 1, perc_constants.MAX_QUEUES do
      mem[q] = memory.createMemPool{
	 ["func"]=function(buf)
	    buf:getPercgPacket():fill{
	       pktLength = DATA_PACKET_SIZE,
	       ethType = eth.TYPE_PERC_DATA}
      end}
      txBufs[q] = mem[q]:bufArray()
   end
   
   local queueInfo = ffi.new("txQueueInfo[?]", perc_constants.MAX_QUEUES+1) -- indexing from 1
   local rxQueue = dev:getRxQueue(perc_constants.ACK_RXQUEUE)
   local rxBufs = memory.bufArray()

   local txCtr = stats:newDevTxCounter(dev, "plain")
   local txQueues = {}
   for q=1,perc_constants.MAX_QUEUES do
      txQueues[q] = dev:getTxQueue(q)
   end

   local lastAckTime = dpdk.getTime()
   local lastPeriodicTime = dpdk.getTime()
   local nextSendTime =  dpdk.getTime() + 0.1
   local nextFlowId = 1

   local numStarted = 0
   local numFinished = 0

   while dpdk.running() and numFinished < numFlows do      
      local dpdkNow = dpdk.getTime()	 

      if dpdkNow > nextSendTime and nextFlowId <= numFlows then
	 -- (get start messages)
	 nextSendTime = dpdkNow	+ 0.001
	 numStarted = numStarted + 1
	 local size = math.ceil(flowSizes:value()/1500.0)
	 local flow = nextFlowId
	 local percgDst = 1
	 assert(next(freeQueues) ~= nil)
	 local q = table.remove(freeQueues)
	 queueInfo[q].flow = flow
	 queueInfo[q].ethSrc = ethSrc
	 queueInfo[q].ethDst = ethDst
	 queueInfo[q].percgSrc = percgSrc
	 queueInfo[q].percgDst = percgDst -- actually device id only
	 queueInfo[q].size = size
	 queueInfo[q].sent = 0
	 queueInfo[q].acked = 0
	 queueInfo[q].active = true
	 queueInfo[q].acked_time = dpdkNow
	 queueInfo[q].start_time = dpdkNow
	 log:info("flow " .. tostring(flow)
		     .. " started (queue " .. tostring(q) .. ")")
	 nextFlowId = nextFlowId + 1
      end -- ends do (get start messages)

      do -- (send data packets)
	 for q=1,perc_constants.MAX_QUEUES do
	    assert(queueInfo[q].size >= queueInfo[q].sent)
	    if queueInfo[q].active
	    and queueInfo[q].sent < queueInfo[q].size then
	       local remaining = queueInfo[q].size - queueInfo[q].sent
	       if (remaining < txBufs[q].maxSize)
	       then
		  txBufs[q]:allocN(DATA_PACKET_SIZE, remaining)
	       else
		  txBufs[q]:allocN(DATA_PACKET_SIZE, txBufs[q].maxSize)
	       end
	       for _, buf in ipairs(txBufs[q]) do
		  local pkt = buf:getPercgPacket()
		  pkt.percg:setSource(queueInfo[q].percgSrc)
		  pkt.percg:setDestination(queueInfo[q].percgDst)
		  pkt.eth:setSrc(queueInfo[q].ethSrc)
		  pkt.eth:setDst(queueInfo[q].ethDst)		  
		  pkt.percg:setFlowId(queueInfo[q].flow) -- 32b -> 16b
		  pkt.payload.uint64[0] = queueInfo[q].flow
		  pkt.payload.uint64[1] = q
		  pkt.payload.uint64[4] = queueInfo[q].size
		  pkt.eth:setSrc(q)
		  pkt.eth:setType(eth.TYPE_PERC_DATA)
		  queueInfo[q].sent = queueInfo[q].sent + 1		  
	       end
	       txCtr:update()
	       txQueues[q]:send(txBufs[q])
	    end
	 end  -- ends for q=1,perc_constants.MAX_QUEUES
      end  -- ends do (send data packets)
      
      do -- (receive acks)
	 local now = dpdk.getTime()
	 local rx = rxQueue:tryRecv(rxBufs, 20)
	 for b = 1 , rx do
	    local pkt = rxBufs[b]:getPercgPacket()
	    local flow = pkt.payload.uint64[0]
	    local q = pkt.payload.uint64[1]
	    local acked = pkt.payload.uint64[2]
	    if (queueInfo[q].active
		   and queueInfo[q].flow == flow
		and pkt.payload.uint64[2] > queueInfo[q].acked) then    
	       queueInfo[q].acked = pkt.payload.uint64[2]
	       queueInfo[q].acked_time = now
	       assert(queueInfo[q].acked <= queueInfo[q].size)
	       if (queueInfo[q].acked == queueInfo[q].size) then
		  queueInfo[q].active = false
		  table.insert(freeQueues, q)
		  local fct = queueInfo[q].acked_time - queueInfo[q].start_time
		  log:info("flow " .. tostring(queueInfo[q].flow)
			      .. " ended (queue " .. tostring(q) .. ")"
			      .. " fct: " .. tostring(fct)
			      .. " size: " .. tostring(queueInfo[q].size)
			      .. " acked: " .. tostring(queueInfo[q].acked))
		  numFinished = numFinished + 1
	       end
	    end
	 end
	 rxBufs:freeAll()
      end -- ends do (receive acks)

      -- timeout flows that haven't received acks in a while
      if dpdkNow > lastAckTime + 1 then
	 lastAckTime = dpdkNow
	 for q=1,perc_constants.MAX_QUEUES do
	    if queueInfo[q].active and
	       (lastAckTime > tonumber(queueInfo[q].acked_time)
		or queueInfo[q].size == queueInfo[q].acked) then
		  log:info("flow " .. tostring(queueInfo[q].flow)
			       .. " timed out (queue " .. q .. ")")
		  queueInfo[q].active = false
		  table.insert(freeQueues, q)
		  numFinished = numFinished + 1
	    end 
	 end -- ends for q=1,..
      end -- ends do
      
   end -- ends while dpdk.running()
   txCtr:finalize()
end

return dataMod
