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
local PercLink = require "examples.perc-moongen-single.perc_link"

local percg = require "proto.percg"
local percc1 = require "proto.percc1"
local eth = require "proto.ethernet"

local ipc = require "examples.perc-moongen-single.ipc"
local perc_constants = require "examples.perc-moongen-single.constants-han1"

local CONTROL_PACKET_SIZE = perc_constants.CONTROL_PACKET_SIZE
local DATA_PACKET_SIZE	= perc_constants.DATA_PACKET_SIZE
local ACK_PACKET_SIZE = perc_constants.ACK_PACKET_SIZE

local isMonitoring = true

ffi.cdef [[
typedef struct lalala {
double start_time;
uint64_t size;
double first_control_send_time;
double first_control_recv_time;
uint16_t first_control_recv_cqueue;
uint16_t first_control_recv_dqueue; 
double first_data_send_start_time;
double first_data_send_end_time;
uint16_t first_data_send_size;
double last_data_send_start_time;
double last_data_send_end_time;
uint64_t last_data_send_size;
uint16_t num_data_sends;
bool sender;
bool timed_out;
} logFlow;

typedef struct foo { bool active; 
uint64_t flow, size, sent;
uint64_t start_time;
uint8_t percgSrc, percgDst;
union mac_address ethSrc, ethDst;
int currentRate, nextRate; double changeTime;}
 txQueueInfo;
typedef struct bar {
uint64_t flow, recv, size, start_time;
uint8_t percgSrc, percgDst;
union mac_address ethSrc, ethDst;}
 rxQueueInfo;
]]

dataMod = {}


function initializePercc1Packet(buf, percgSrc, ethSrc,
				percgDst, ethDst,
			        flowId)
   local pkt = buf:getPercc1Packet()
   pkt:fill{
      pktLength = CONTROL_PACKET_SIZE,
      percgSource = percgSrc, -- TO CHANGE
      percgDestination = percgDst, -- TO CHANGE
      percgFlowId = flowId, -- TO CHANGE
      percgIsData = percg.PROTO_CONTROL,
      percc1IsForward = percc1.IS_FORWARD,
      percc1IsExit = percc1.IS_NOT_EXIT,
      percc1Hop = 0,
      percc1MaxHops = 0
   }
   pkt.eth:setSrcString(perc_constants["ethAddrStr"..percgSrc])
   pkt.eth:setDstString(perc_constants["ethAddrStr"..percgDst])
   pkt.eth:setType(eth.TYPE_PERCG)
end

function commonControlProcess(pkt)
   do
      local tmp = pkt.eth:getDst()
      pkt.eth:setDst(pkt.eth:getSrc())
      pkt.eth:setSrc(tmp)
   end

   do
      local tmp = pkt.percg:getDestination()
      pkt.percg:setDestination(pkt.percg:getSource())
      pkt.percg:setSource(tmp)
   end
   
   -- get maxHops, then smallest index, two rates
   local maxHops = pkt.percc1:getHop()
   if (pkt.percc1:IsForward()) then -- direction of received packet = fwd -> received at receiver
      -- maxHops is whatever source set, could be 0 for new packet or actual maxHops for old packet
      -- from sender
      -- do nothing
   else
      assert(pkt.percc1:getHop() == 0) -- direction of received packet = rev -> received at sender
      maxHops = pkt.percc1:getMaxHops()
   end

   -- log:info("received control packet "
   -- 	       .. pkt.percg:getString()
   -- 	       .. "\n" .. pkt.percc1:getString()
   -- 	       .. "\n")
   
   local bnInfo = pkt.percc1:getBottleneckInfo(maxHops)
   local bnRate1, bnRate2 = bnInfo.bnRate1, bnInfo.bnRate2   
   -- log:info("for flow " .. pkt.percg:getFlowId()
   -- 	       .. " bottleneck rate is " .. bnRate1
   -- 	       .. " based on info from " .. maxHops
   -- 	       .. " hops.\n")

   local bnBitmap = bnInfo.bnBitmap
   assert(bnRate1 ~= nil)
   assert(bnRate2 ~= nil)
   assert(bnBitmap ~= nil)
   -- then set rate at each index
   -- and unsat/ sat at each index
   --pkt.percg:setRatesAndLabelGivenBottleneck(rate, hop, maxHops)	      
   for i=1,maxHops do		 
      pkt.percc1:setOldLabel(i, pkt.percc1:getNewLabel(i))
      pkt.percc1:setOldRate(i,  pkt.percc1:getNewRate(i))
      if bnBitmap[i] ~= 1 then
	 pkt.percc1:setNewLabel(i, percc1.LABEL_SAT)
	 pkt.percc1:setNewRate(i,  bnRate1)
	 -- log:info("for flow " .. pkt.percg:getFlowId()
	 -- 	  .. " setting new demand at hop " .. i
	 -- 		   .. " to " .. bnRate1)
      else
	 pkt.percc1:setNewLabel(i, percc1.LABEL_UNSAT)
	 pkt.percc1:setNewRate(i, bnRate2)
	 -- log:info("for flow " .. pkt.percg:getFlowId()
	 -- 	  .. " setting new demand at hop " .. i
	 -- 		   .. " to " .. bnRate1)
      end
   end -- for i=1,maxHops
   -- receiver sets it to number of hops
   -- sender also sets it to number of hops (that was set by receiver)
   pkt.percc1:setMaxHops(maxHops) -- and hop is the same

   do -- flip direction of received packet
      if (pkt.percc1:getIsForward() ~= percc1.IS_FORWARD) then
	 pkt.percc1:setIsForward(percc1.IS_FORWARD)
	 pkt.percc1:setMaxHops(0) -- sender resets maxHops
      else
	 pkt.percc1:setIsForward(percc1.IS_NOT_FORWARD)
      end
   end
   
   return bnRate1
end


function receiverControlProcess(pkt)
   commonControlProcess(pkt)
end

function senderControlProcess(pkt, queueInfo, dpdkNow)
   if (pkt.percc1:IsExit()) then
      --print("sender got exit packet, set ether type to DROP")
      pkt.eth:setType(eth.TYPE_DROP)
      return
   end
   
   local newRate = math.floor(commonControlProcess(pkt))
   
   if (queueInfo == nil) then
      --print("sender got packet for completed flow, set IsExit")
      pkt.percc1:setIsExit(percc1.IS_EXIT)
      return
   end

   -- update rate info
   assert(newRate ~= nil)
   assert(queueInfo.currentRate ~= -1)
   if (newRate < queueInfo.currentRate
       or queueInfo.currentRate == perc_constants.startRate) then -- start with new rate ASAP
      queueInfo.nextRate = newRate
      queueInfo.changeTime = dpdkNow
   elseif (newRate == queueInfo.currentRate) then
      queueInfo.nextRate = -1
      queueInfo.changeTime = -1
   else
      -- log:info("flow " .. pkt.percg:getFlowId() 
      -- 		  .. " can increase rate from "
      -- 		  .. tostring(queueInfo.currentRate)
      -- 		  .. " to " .. tostring(newRate))

      -- TODO(lav): not perfectly conservative but that's ok
      if (queueInfo.nextRate == -1) then
      	 queueInfo.nextRate = newRate
      	 queueInfo.changeTime = dpdkNow + 2 * perc_constants.rtts
      else
	 queueInfo.nextRate = newRate
	 -- changeTime as is
      end

      -- if (queueInfo.nextRate == -1) then
      -- 	 queueInfo.nextRate = newRate
      -- 	 queueInfo.changeTime = dpdkNow + 2 * perc_constants.rtts
      -- elseif (newRate <= queueInfo.nextRate) then
      -- 	 queueInfo.nextRate = newRate
      -- else
      -- 	 assert(newRate > queueInfo.nextRate)
      -- 	 queueInfo.nextRate = newRate
      -- 	 queueInfo.changeTime = dpdkNow + 2 * perc_constants.rtts
      -- end
   end
   return
end

-- sends data packets and receives acks
function dataMod.dataSlave(dev, cdfFilepath, scaling, interArrivalTime, numFlows,
			 percgSrc, ethSrc,
			 tableDst, 
			 isSending, isReceiving,
			 readyInfo)
   local thisCore = dpdk.getCore()   

   if type(ethSrc) == "number" then
      --local buf = ffi.new("char[20]")
      --dpdkc.get_mac_addr(ethSrc, buf)
      local ethSrcStr = perc_constants["ethAddrStr"..ethSrc]	
      assert(ethSrcStr ~= nil)
      --ffi.string(buf)      
      ethSrc = parseMacAddress(ethSrcStr)
      if (ethSrc == nil) then log:warn("couldn't parse " .. ethSrcStr) end
      assert(ethSrc ~= nil)
   elseif istype(macAddrType, ethSrc) then
      ethSrc = ethSrc
   else
      assert(false)
   end

   log:info("Data slave running on "
	       .. " dev " .. dev.id 
	       .. " MAC addr " .. tostring(ethSrc)
	       .. ", core " .. thisCore
	       .. ", isSending " .. tostring(isSending)
	       .. ", isReceiving " .. tostring(isReceiving)
	       .. "\n")

   if isSending == false then
      assert(cdfFilepath == nil)
      assert(numFlows == nil)
      assert(tableDst == nil)
   end

   if isSending then
      assert(cdfFilepath ~= nil)
      assert(numFlows ~= nil)
      assert(tableDst ~= nil)
   end

   if isSending or isReceiving then
      assert(dev ~= nil)
      assert(ethSrc ~= nil)
      assert(percgSrc ~= nil)
      assert(readyInfo ~= nil)
   end
   
   -- sender variables for generating new
   -- data and control packets
   local flowSizes = nil
   local ethDst = nil
   local percgDst = nil

   if isSending then
      flowSizes = fsd.create()
      flowSizes:loadCDF(cdfFilepath)
      local avgFlowSize = flowSizes:avg()
      assert(avgFlowSize > 0)
      log:info("loaded flow sizes file with avg. flow size "
		  .. tostring(avgFlowSize/1500) .. " packets, will scale by "
		  .. tostring(scaling))

      percgDst, ethDst = next(tableDst)
      if type(ethDst) == "number" then
	 --local buf = ffi.new("char[20]")
	 --dpdkc.get_mac_addr(ethDst, buf)
	 local ethDstStr = perc_constants["ethAddrStr"..ethDst]
	 -- ffi.string(buf)      
	 ethDst = parseMacAddress(ethDstStr)
      elseif istype(macAddrType, ethDst) then
	 ethDst = ethDst
      else
	 assert(false)
      end
   end

   -- common variables
   -- for control packets processing
   local cMem = nil
   local cBufs = nil
   local cRxQueue = nil
   local link = nil

   -- link statistics
   local txCtr = nil
   local rxCtr = nil
   
   -- sending thread's variables
   local cNewMem = nil
   local cNewBufs = nil

   local mem = nil
   local txBufs = nil
   local freeQueues = nil
   local queueInfo = nil


   local lastPeriodicTime = nil
   local nextSendTime = nil
   local nextFlowId = nil
   local numStarted = nil
   local numFinished = nil

   -- receiving thread's variables
   local rxQueueInfo = nil
   local dataRxQueue = nil
   local dataRxBufs = nil

   -- monitoring
   local logFlowInfo = nil
   if isMonitoring then
      logFlowInfo = ffi.new("logFlow[?]",
			    1000, {})
      end
   -- To receive and reply to control packets
   if isSending or isReceiving then
      cMem = memory.createMemPool()
      cBufs = memory.bufArray()   
      cRxQueue = dev:getRxQueue(perc_constants.CONTROL_RXQUEUE)
      -- CONTROL_TX_QUEUE   
      link = PercLink:new()
      -- link statistics, and all tx queues      
      txCtr = stats:newDevTxCounter(dev, "plain")
      txQueues = {}
      for q=1,perc_constants.MAX_QUEUES do
	 txQueues[q] = dev:getTxQueue(q)
      end
      rxCtr = stats:newDevRxCounter(dev, "plain", "rx-throughput-".. dev.id .. "-txt")
   end
   
   if isSending then
      -- To send new control packets and receive/ responde
      -- to existing control packets
      cNewMem = memory.createMemPool{
      	 ["func"]=function(buf)
	    buf:getPercgPacket():fill{
	       pktLength = CONTROL_PACKET_SIZE,
	       ethType = eth.TYPE_PERCG}
      end}
      cNewBufs = cNewMem:bufArray()
      -- NEW_CONTROL_TXQUEUE
   end

   if isReceiving then
      rxQueueInfo =
	 ffi.new("rxQueueInfo[?]",
		 perc_constants.MAX_SENDERS*perc_constants.MAX_QUEUES+1, {}) -- indexing from , one per src
      -- invariants
      for q=1,perc_constants.MAX_SENDERS*perc_constants.MAX_QUEUES do
	 rxQueueInfo[q].recv = 0ULL
	 rxQueueInfo[q].size = 0ULL
	 rxQueueInfo[q].start_time = 0ULL
      end

      dataRxQueue = dev:getRxQueue(perc_constants.DATA_RXQUEUE)
      dataRxBufs = memory.bufArray()
      -- ACK_TXQUEUE
   end

   if isSending then
      -- To send data and receive ACKs
      mem = {}
      txBufs = {}
      freeQueues = {}
      for i=1, perc_constants.MAX_QUEUES do
	 if i ~= perc_constants.CONTROL_TXQUEUE
	    and i ~= perc_constants.NEW_CONTROL_TXQUEUE
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
		  ethType = eth.TYPE_PERC_DATA,
		  ["n"] = 65535}
	 end}
	 txBufs[q] = mem[q]:bufArray()
      end
      
      queueInfo = ffi.new("txQueueInfo[?]", perc_constants.MAX_QUEUES+1) -- indexing from 1

      lastPeriodicTime = dpdk.getTime()
      nextSendTime =  dpdk.getTime() --+ interArrivalTime
      nextFlowId = 1
      
      numStarted = 0
      numFinished = 0
   end

   ipc.waitTillReady(readyInfo)
   if isSending and (isReceiving == false) then
      dpdk.sleepMillis(500)
   end
   
   -- a thread that's receiving runs forever
   -- a thread that's only sending stops as soon as all finish
   while dpdk.running() and
      ((isSending and numFinished < numFlows)
	 or isReceiving)  do
	 local dpdkNow = dpdk.getTime()
	 
	 if isSending then
	    -- print("core " .. thisCore .. " must wait for " .. (nextSendTime - dpdkNow)
	    --	     .. " s so it can start flow # " .. nextFlowId .. " / " .. numFlows .. "\n")
	    if dpdkNow > nextSendTime and nextFlowId <= numFlows then
	       -- print("core " .. thisCore .. " starting flow # " .. nextFlowId)
	       -- (get start messages)
	       nextSendTime = dpdkNow	+ interArrivalTime
	       numStarted = numStarted + 1
	       local size = math.ceil((flowSizes:value() * scaling)/1500.0)
	       local flow = nextFlowId
	       local percgDst = 1
	       assert(next(freeQueues) ~= nil)
	       local q = table.remove(freeQueues)
	       assert(q ~= nil)
	       queueInfo[q].flow = flow
	       queueInfo[q].percgSrc = percgSrc
	       queueInfo[q].percgDst = percgDst -- actually device id only
	       queueInfo[q].size = size
	       queueInfo[q].sent = 0
	       queueInfo[q].active = true
	       queueInfo[q].start_time = (dpdkNow * 1e6)
	       assert(perc_constants.startRate ~= nil)
	       queueInfo[q].currentRate = perc_constants.startRate --dev:getTxQueue(q):getTxRate()
	       log:info("setting rate of flow " .. tostring(flow) ..
			   " to " .. (perc_constants.startRate*1e3) .. "Kb/s")
	       dev:getTxQueue(q):setRate(perc_constants.startRate) -- start blasting right away v/s trickling right away
	       queueInfo[q].nextRate = -1
	       queueInfo[q].changeTime = -1
	       log:info("flow " .. tostring(flow)
			   .. " started (queue " .. tostring(q) .. ")")


	       -- send  a new control packet
	       cNewBufs:allocN(CONTROL_PACKET_SIZE, 1)
	       initializePercc1Packet(cNewBufs[1], percgSrc, ethSrc,
				      percgDst, ethDst, flow)
	       local pkt = cNewBufs[1]:getPercc1Packet()
	       pkt.payload.uint64[0] = q
	       queueInfo[q].ethSrc = pkt.eth.src
	       queueInfo[q].ethDst = pkt.eth.dst
	       --link:processPercc1Packet(pkt)
	       -- stored in host or network order??
	       -- if (true) then
	       -- 	  print("dev " .. dev.id .. " sent new control packet from "
	       -- 		   .. pkt.eth:getSrcString()
	       -- 		   .. " to " .. pkt.eth:getDstString()
	       -- 		   .. " of type " .. pkt.eth:getType()
	       -- 		   .. ", hops: " .. pkt.percc1:getHop()
	       -- 		   .. ", maxHops: " .. pkt.percc1:getMaxHops())
	       -- end		    
	       pkt.percc1:doHton()
	       logFlowInfo[flow].first_control_send_time = dpdk.getTime()
	       txQueues[perc_constants.NEW_CONTROL_TXQUEUE]:send(cNewBufs)
	       nextFlowId = nextFlowId + 1
	    end -- ends do (get start messages)
	 end -- ends IFSENDING

	 if isReceiving or isSending then -- receive control packets, process and send back
	    local rx = cRxQueue:tryRecv(cBufs, 100)
	    -- if rx > 0 then
	    --    print("trying to receive " .. rx .." control packets on dev "
	    -- 		.. cRxQueue.id .. ", queue " .. cRxQueue.qid
	    -- 		.. "\n")
	    -- end
	    for b = 1, rx do
	       local pkt = cBufs[b]:getPercc1Packet()
	       pkt.percc1:doNtoh()
	       -- if (true) then
	       -- 	  print("dev " .. dev.id .. " got control packet from "
	       -- 		   .. pkt.eth:getSrcString()
	       -- 		   .. " to " .. pkt.eth:getDstString()
	       -- 		   .. " forward? " .. tostring(pkt.percc1:IsForward())
	       -- 		   .. ", hops: " .. pkt.percc1:getHop()
	       -- 		   .. ", maxHops: " .. pkt.percc1:getMaxHops())
	       -- end		    

	       if pkt.percc1:IsForward() == false then
		  -- if (true) then print (" ingress link processing at dev " .. dev.id) end
		  --link:processPercc1Packet(pkt)
	       end
	       
	       if pkt.percc1:IsForward() then
		  receiverControlProcess(pkt) -- At this point, receiver sets packet direction to reverse
	       else
		  assert(isSending)
		  if (isSending) then
		     local q = tonumber(pkt.payload.uint64[0])
		     local qi = queueInfo[q]		     
		     if qi.active == false then
			qi = nil
		     elseif (tonumber(qi.flow) ~= pkt.percg:getFlowId()) then
			qi = nil
		     else
			if isMonitoring then
			   local flowId = tonumber(qi.flow)
			   logFlowInfo[flowId].first_control_recv_time
			      = dpdk.getTime()
			   logFlowInfo[flowId].first_control_recv_cqueue
			      = pkt.percc1:getControlQueueSize(2)
			   logFlowInfo[flowId].first_control_recv_dqueue
			      = pkt.percc1:getDataQueueSize(2)
			   end
			-- qi is correct when queue is valid and flow matches
		     end
		     senderControlProcess(pkt, qi, dpdkNow) -- At this point sender sets packet direction forward
		     if qi ~= nil and qi.nextRate ~= -1 and qi.changeTime <= dpdkNow then
			log:info("setting rate of flow " .. tostring(qi.flow) ..
				    " to " .. qi.nextRate .. " Kb/s")
			local rate_mbps = qi.nextRate * 1e-3
			txQueues[q]:setRate(rate_mbps)
			qi.currentRate = qi.nextRate
			qi.nextRate = -1
			qi.changeTime = -1
		     end
		     assert(pkt.percc1:IsForward() or pkt.eth:getType() == eth.TYPE_DROP)
		     if (pkt.eth:getType() ~= eth.TYPE_DROP) then
			-- if (true) then print (" egress link processing at dev " .. dev.id) end
			--link:processPercc1Packet(pkt)
		     end
		  end -- ends IFSENDING
	       end
	       pkt.percc1:doHton()
	    end
	    txQueues[perc_constants.CONTROL_TXQUEUE]:sendN(cBufs, rx)
	 end

	 if isSending then
	    do -- (send data packets)
	       for q=1,perc_constants.MAX_QUEUES do
		  assert(queueInfo[q].size >= queueInfo[q].sent)
		  if queueInfo[q].active
		  and queueInfo[q].sent < queueInfo[q].size then		     
		     local remaining = queueInfo[q].size - queueInfo[q].sent

		     -- for monitoring..
		     local flow = tonumber(queueInfo[q].flow)
		     local flowStarting = (queueInfo[q].sent == 0)
		     local flowEnding = (remaining < txBufs[q].maxSize)

		     if (remaining < txBufs[q].maxSize)
		     then
			txBufs[q]:allocN(DATA_PACKET_SIZE, remaining)
		     else
			txBufs[q]:allocN(DATA_PACKET_SIZE, txBufs[q].maxSize)
		     end
		     for bufNo, buf in ipairs(txBufs[q]) do
			local pkt = buf:getPercgPacket()
			pkt.percg:setSource(queueInfo[q].percgSrc)
			pkt.percg:setDestination(queueInfo[q].percgDst)
			pkt.eth.src = queueInfo[q].ethSrc
			pkt.eth.dst = queueInfo[q].ethDst
			pkt.percg:setFlowId(queueInfo[q].flow) -- 32b -> 16b
			pkt.payload.uint64[0] = queueInfo[q].flow
			pkt.payload.uint64[1] = q
			pkt.payload.uint64[2] = queueInfo[q].start_time
			pkt.payload.uint64[4] = queueInfo[q].size
			pkt.eth:setType(eth.TYPE_PERC_DATA)
			queueInfo[q].sent = queueInfo[q].sent + 1
		     end -- ends for bufNo,..

		     
		     if isMonitoring then
			if flowStarting then
			   logFlowInfo[flow].sender = true
			   logFlowInfo[flow].num_data_sends = 0
			   logFlowInfo[flow].size = queueInfo[q].size
			   logFlowInfo[flow].first_data_send_start_time = dpdk.getTime()
			   logFlowInfo[flow].first_data_send_size = txBufs[q].size
			end
			if flowEnding then
			   logFlowInfo[flow].last_data_send_start_time = dpdk.getTime()
			   logFlowInfo[flow].last_data_send_size = txBufs[q].size
			end
			txQueues[q]:send(txBufs[q])
			logFlowInfo[flow].num_data_sends =
			   logFlowInfo[flow].num_data_sends + 1			
			if flowStarting then
			   logFlowInfo[flow].first_data_send_end_time = dpdk.getTime()
			end
			if flowEnding then
			   logFlowInfo[flow].last_data_send_end_time = dpdk.getTime()
			end
		     else -- just send if not monitoring
			   txQueues[q]:send(txBufs[q])
		     end		     			

		     if (queueInfo[q].sent == queueInfo[q].size) then
			queueInfo[q].active = false
			table.insert(freeQueues, q)
			numFinished = numFinished + 1
		     end
		     
		  end -- ends if active
	       end  -- ends for q=1,perc_constants.MAX_QUEUES
	       --txCtr:update()
	    end  -- ends do (send data packets)	    
	 end -- ends IFSENDING


	 if isReceiving then
	    do -- receive data packets and send ACKs	       
	       do
		  local rx = dataRxQueue:recv(dataRxBufs)		  
		  for b = 1, rx do
		     local buf = dataRxBufs[b]
		     local pkt = buf:getPercgPacket()
		     local flow = pkt.payload.uint64[0]
		     local q = pkt.payload.uint64[1]
		     local start_time = pkt.payload.uint64[2]
		     local size = pkt.payload.uint64[4]
		     -- if (b == 1) then
		     -- 	print("dev " .. dev.id .. " got data packet from "
		     --   		 .. pkt.eth:getSrcString()
		     --   		 .. " to " .. pkt.eth:getDstString())
		     -- end
		     q = (pkt.percg:getSource() * perc_constants.MAX_QUEUES) + q
		     if rxQueueInfo[q].flow ~= flow then
			rxQueueInfo[q].flow = flow 
			rxQueueInfo[q].recv = 0ULL
			rxQueueInfo[q].size = size
			rxQueueInfo[q].start_time = start_time
			rxQueueInfo[q].ethSrc = pkt.eth.src
			rxQueueInfo[q].ethDst = pkt.eth.dst
			rxQueueInfo[q].percgSrc = pkt.percg:getSource()
			rxQueueInfo[q].percgDst = pkt.percg:getDestination()
		     end
		     -- assert(seqNo < size)
		     assert(rxQueueInfo[q].size == size)	   
		     rxQueueInfo[q].recv = rxQueueInfo[q].recv + 1
		     if (rxQueueInfo[q].recv == rxQueueInfo[q].size) then
			local fct = dpdkNow*1e6 - rxQueueInfo[q].start_time
			local fct_us = fct
			local size = rxQueueInfo[q].size
			local norm_fct = tonumber(fct/(size*1.2))
			log:info("flow " .. tostring(pkt.percg:getSource() * 10000ULL + rxQueueInfo[q].flow)
				    .. " ended (queue " .. tostring(q) .. ")"
				    .. " fct: " .. tostring(fct)
				    .. " size: " .. tostring(size)
				    .. " received: " .. tostring(rxQueueInfo[q].recv)
				    .. " fct_us: " .. tostring(fct_us)
				    .. " norm_fct: " .. string.format("%.1f",norm_fct)
			)
		     end
		     local recvd = rxQueueInfo[q].recv
		     local total = rxQueueInfo[q].size
		     if(recvd > total) then 
			log:warn("for flow " .. tostring(flow)
				    .. " recvd " .. tostring(recvd)
				 .. " more than size " .. tostring(size)) end
		     assert(rxQueueInfo[q].recv <= rxQueueInfo[q].size)	    
		  end
		  if rx > 0 then
		     rxCtr:update()
		     dataRxBufs:freeAll()
		  end	 		  
	       end -- ends fo for receiving data and sending ACKs
	    end -- ends if Receiving
	 end -- ends while dpdk.running()
   end
   rxCtr:finalize()
   txCtr:finalize()

   if isMonitoring then
      for i = 0, 999 do
	 local info = logFlowInfo[i]
	 if info.sender == true and info.timed_out == false then
	    print("flow " .. tostring(i)
			.. " start_time " .. tostring(info.start_time)
			.. " size " .. tostring(info.size)
			.. " first_control_send_time " .. tostring(info.first_control_send_time)
			.. " first_control_recv_time " .. tostring(info.first_control_recv_time)
			.. " first_control_recv_cqueue " .. tostring(info.first_control_recv_cqueue)
			.. " first_control_recv_dqueue " .. tostring(info.first_control_recv_dqueue)
			.. " first_data_send_start_time " .. tostring(info.first_data_send_start_time)
			.. " first_data_send_end_time " .. tostring(info.first_data_send_end_time)
			.. " first_data_send_size " .. tostring(info.first_data_send_size)
			.. " last_data_send_start_time " .. tostring(info.last_data_send_start_time)
			.. " last_data_send_end_time " .. tostring(info.last_data_send_end_time)
			.. " last_data_send_size " .. tostring(info.last_data_send_size)
			.. " num_data_sends " .. tostring(info.num_data_sends)
			.. " sender " .. tostring(info.sender)
			.. " timed_out " .. tostring(info.timed_out)
	    )
	 end
      end
   end
   
end

return dataMod

