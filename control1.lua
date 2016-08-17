local ffi = require("ffi")
local pkt = require("packet")
local dpdk	= require "dpdk"
local memory	= require "memory"
local device	= require "device"
local stats		= require "stats"
local pipe		= require "pipe"

local percg = require "proto.percg"
local percc1 = require "proto.percc1"
local eth = require "proto.ethernet"

local ipc = require "examples.perc-moongen.ipc"
local monitor = require "examples.perc-moongen.monitor"
local perc_constants = require "examples.perc-moongen.constants"

local Link1 = require "examples.perc-moongen.perc_link"

local CONTROL_PACKET_SIZE	= perc_constants.CONTROL_PACKET_SIZE
local ff64 = 0xFFFFFFFFFFFFFFFF

controlMod = {}

ffi.cdef [[
typedef struct {
double currentRate, nextRate;
double changeTime;
bool valid;
} rateInfo;
]]

-- don't have to store this at host, in packets only
-- uint8_t percgSrc, percgDst;
-- union mac_address ethSrc, ethDst;


function controlMod.warn(str)
   if perc_constants.WARN_CONTROL then
      print("control2.lua warn: " .. str)
   end
end

function controlMod.log(str)
   if perc_constants.LOG_CONTROL then
      print(str)
   end
end

function controlMod.percc1ProcessAndGetRate(pkt)
   local tmp = pkt.percg:getDestination()
   pkt.percg:setDestination(pkt.percg:getSource())
   pkt.percg:setSource(tmp)

   -- get maxHops, then smallest index, two rates
   local maxHops = pkt.percc1:getHop()
   if (pkt.percc1:getIsForward() ~= percc1.IS_FORWARD) then
      maxHops = pkt.percc1:getMaxHops()
   end
   local bnInfo = pkt.percc1:getBottleneckInfo(maxHops)
   local bnRate1, bnRate2 = bnInfo.bnRate1, bnInfo.bnRate2   
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
	 -- controlMod.log("setting new rate of " .. i
	 -- 	  .. " to " .. bnRate1)
      else
	 pkt.percc1:setNewLabel(i, percc1.LABEL_UNSAT)
	 pkt.percc1:setNewRate(i, bnRate2)
	 -- controlMod.log("setting new rate of " .. i
	 -- 	  .. " to " .. bnRate2)
      end
   end -- for i=1,maxHops
   pkt.percc1:setMaxHops(maxHops) -- and hop is the same
   if (pkt.percc1:getIsForward() ~= percc1.IS_FORWARD) then
      --controlMod.log("marking packet as forward")
      pkt.percc1:setIsForward(percc1.IS_FORWARD)
   else
      --controlMod.log("marking packet as reverse")
      pkt.percc1:setIsForward(percc1.IS_NOT_FORWARD)
   end -- if (pkt.percc1:getIsForward() ..
   return bnRate1
end

function initializePercc1Packet(buf)
   buf:getPercc1Packet():fill{
      pktLength = CONTROL_PACKET_SIZE,
      percgSource = 0, -- TO CHANGE
      percgDestination = 1, -- TO CHANGE
      percgFlowId = 0, -- TO CHANGE
      percgIsData = percg.PROTO_CONTROL,
      percc1IsForward = percc1.IS_FORWARD,
      percc1IsExit = percc1.IS_NOT_EXIT,
      percc1Hop = 0,
      percc1MaxHops = 0,
      ethSrc = 0, -- TO CHANGE
      ethDst = "10:11:12:13:14:15", -- TO CHANGE
      ethType = eth.TYPE_PERCG}
end

function controlMod.controlSlave(dev, pipes, readyInfo, monitorPipe)
      local thisCore = dpdk.getCore()
      controlMod.log("Running control slave on core " .. thisCore)
      local egressLink = Link1:new()      
      local mem = memory.createMemPool()
      
      local rxQueue = dev:getRxQueue(perc_constants.CONTROL_RXQUEUE)
      local txQueue = dev:getTxQueue(perc_constants.CONTROL_TXQUEUE)
      local newTxQueue = dev:getTxQueue(perc_constants.NEW_CONTROL_TXQUEUE)
      assert(rxQueue ~= nil)
      assert(txQueue ~= nil)
      assert(newTxQueue ~= nil)
      
      local freeQueues = {}
      local queues = {}
      local queueRates = ffi.new("rateInfo[?]", 129)
      
      for i=1, perc_constants.MAX_QUEUES do
	 if i ~= perc_constants.CONTROL_TXQUEUE
	    and i ~= perc_constants.NEW_CONTROL_TXQUEUE
	    and i ~= perc_constants.ACK_TXQUEUE
	 and i ~= perc_constants.DROP_QUEUE then 
	    table.insert(freeQueues, i)
	 end
	 queueRates[i].currentRate = 1
	 queueRates[i].nextRate = -1
	 queueRates[i].changeTime = -1
	 queueRates[i].valid = false
      end

      local bufs = memory.bufArray()
      -- to rx packets and modify and tx
      -- hope it's okay to use sam bufs for rx and tx
      
      local newBufs = mem:bufArray()
      -- for packets sent out for new flows
      ipc.waitTillReady(readyInfo)
      controlMod.log("ready to start control2")

       while dpdk.running() do
	  local dpdkLoopStartTime = dpdk.getTime()	  	  
	  do -- echoes received packets
	     local rx = rxQueue:tryRecv(bufs, 128)
	     local now = dpdk.getTime()

	     for i = 1, rx do
		local pkt = bufs[i]:getPercc1Packet()
		pkt.percc1:doNtoh()

		-- ingress link processing for reverse packets (called egress)
		if pkt.percc1:getIsForward() == percc1.IS_NOT_FORWARD then
		   egressLink:processPercc1Packet(pkt)		   
		end
		
		local tmp = pkt.eth:getDst()
		pkt.eth:setDst(pkt.eth:getSrc())
		pkt.eth:setSrc(tmp)

		-- handle differently at receiver and source
		-- receiver simply processes and echoes, FIN or not
		if pkt.percc1:getIsForward() == percc1.IS_FORWARD then
		   local flowId = pkt.percg:getFlowId()
		   assert(flowId >= 100)
		   controlMod.percc1ProcessAndGetRate(pkt)		   
		else
		   local flowId = pkt.percg:getFlowId()	       
		   assert(flowId > 0)
		   -- source doesn't reply to FIN
		   -- will set FIN for flows that ended (data fin-acked)
		   -- will update rates otherwise
		   if pkt.percc1:getIsExit() == percc1.IS_EXIT then
		      pkt.eth:setType(eth.TYPE_DROP)		      
		   elseif queues[flowId] == nil then
		      -- flow timed out by data thread
		      -- so we got a "Fin Ack" and deallocated queues
		      controlMod.percc1ProcessAndGetRate(pkt)
		      assert(pkt.percc1:getIsForward() == percc1.IS_FORWARD)
		      pkt.percc1:setIsExit(percc1.IS_EXIT)	      
		   else -- flow hasn't ended yet, update rates
		      local rate1 = controlMod.percc1ProcessAndGetRate(pkt)
		      local queueNo = queues[flowId]
		      assert(pkt.percc1:getIsForward() == percc1.IS_FORWARD)
		      assert(rate1 ~= nil)
		      assert(queueNo ~= nil)
		      local rateInfo = queueRates[queueNo]
		      assert(queueRates[queueNo].valid)		  
		      assert(rateInfo.currentRate >= 0)
		      if rate1 ~= rateInfo.currentRate then
			 if rate1 < rateInfo.currentRate then
			    rateInfo.currentRate = rate1
			    rateInfo.nextRate = -1
			    rateInfo.changeTime = -1
			    local dTxQueue = dev:getTxQueue(queueNo)
			    local configuredRate = rate1
			    assert(rate1 < perc_constants.END_HOST_LINK_MBPS)
			    dTxQueue:setRate(configuredRate)
			 else -- rate1 > rateInfo[0].currentRate
			    if rateInfo.nextRate == -1 then
			       rateInfo.nextRate = rate1
			       rateInfo.changeTime = now + 100e-6
			    elseif rateInfo.nextRate == rate1 then
			    elseif rateInfo.nextRate >= 0
			    and rate1 < rateInfo.nextRate then
			       rateInfo.nextRate = rate1
			    else -- rate1 > rateInfo.nextRate
			       rateInfo.nextRate = rate1
			       rateInfo.changeTime = now + 100e-6
			    end 
			 end
		      end -- if rate1 is different from current (rate update?)
		   end 
		   assert(pkt.percc1:getIsForward() == percc1.IS_FORWARD
			     or pkt.eth:getType() == eth.TYPE_DROP)
		end -- if packet is forward
		
		-- egress link processing after sending out forward packets
		if (pkt.eth:getType() ~= eth.TYPE_DROP and
		    pkt.percc1:getIsForward() == percc1.IS_FORWARD) then
		   egressLink:processPercc1Packet(pkt)
		end
		pkt.percc1:doHton()
	     end -- for i = 1, rx
	     txQueue:sendN(bufs, rx)
	  end -- do ECHOES RECEIVED PACKETS


	  do -- MAKES NEW PACKETS
	     -- makes new packets
	     local now = dpdk.getTime()
	     local msgs = ipc.fastAcceptMsgs(
		pipes, "fastPipeAppToControlStart",
		"pFacStartMsg", 20)
	     if next(msgs) ~= nil then
		local numNew = 0
		for msgNo, msg in pairs(msgs) do
		   numNew = numNew + 1
		end
		assert(numNew < newBufs.maxSize)
		newBufs:allocN(CONTROL_PACKET_SIZE, numNew)

		numNew = 0
		for msgNo, msg in pairs(msgs) do
		   if (next(freeQueues) == nil) then
		      ipc.sendFcaResourceExhaustedMsg(
			 pipes, msg.flow)
		   else
		      local flowId = msg.flow
		      assert(flowId ~= nil)
		      assert(flowId > 0)
		      assert(queues[flowId] == nil)
		      local queue = table.remove(freeQueues)
		      assert(queue ~= nil)
		      queues[flowId] = queue
		      queueRates[queue].valid = true
		      queueRates[queue].currentRate = 1
		      local configuredRate = queueRates[queue].currentRate
		      local dTxQueue = dev:getTxQueue(queue)
		      dTxQueue:setRate(configuredRate)		   	  
		      assert(numNew < newBufs.size)
		      numNew = numNew + 1
		      -- tell data thread
		      ipc.sendFcdStartMsg(pipes, msg.flow, msg.size, queue,
					  msg.percgSrc,
					  msg.percgDst,
					  msg.ethSrc,
					  msg.ethDst)
		      -- TODO(lav): remove after fixing the uninit. mbuf bug
		      initializePercc1Packet(newBufs[numNew]) -- re-initialized
		      local pkt = newBufs[numNew]:getPercc1Packet()
		      -- sanity checking that fields have default values
		      assert(pkt.percc1:getNumUnsat(1) == 0)
		      assert(pkt.percc1:getNumUnsat(2) == 0)
		      assert(flowId >= 0)
		      pkt.percg:setFlowId(flowId)
		      pkt.percg:setSource(msg.percgSrc)
		      pkt.percg:setDestination(msg.percgDst)
		      pkt.eth:setSrc(msg.ethSrc)
		      pkt.eth:setDst(msg.ethDst)

		      assert(pkt.eth:getType() ~= eth.TYPE_DROP)
		      assert(pkt.percc1:getIsForward() == percc1.IS_FORWARD)
		      egressLink:processPercc1Packet(pkt)
		      pkt.percc1:doHton()
		   end -- if next(freeQueues)..
		end -- for msgNo, msg..
		newTxQueue:sendN(newBufs, numNew)
	     end -- if msgs ~= nil
	  end -- ends do
       
	  do -- DEALLOCATE QUEUES
	     -- deallocates queues for completed flows
	     -- actually, flows timed out by data thread
	     local msgs =
		ipc.fastAcceptMsgs(
		   pipes, "fastPipeDataToControlFinAck",
		   "pFdcFinAckMsg", 20)
	     if next(msgs) ~= nil then
		for msgNo, msg in pairs(msgs) do
		   local flowId = msg.flow
		   assert(flowId >= 100)
		   local queueNo = queues[flowId]
		   assert(queueNo ~= nil)
		   if queueNo ~= nil then
		      queueRates[queueNo].valid = false
		      queueRates[queueNo].currentRate = 1
		      queueRates[queueNo].nextRate = -1
		      queueRates[queueNo].changeTime = -1
		      table.insert(freeQueues, queueNo)
		      queues[flowId] =  nil
		      ipc.sendFcaFinAckMsg(pipes, msg.flow, msg.size,
					   msg.endTime)
		   else
		      controlMod.warn("Got a FINACK message from data "
				      .. "for flow " .. flowId
					 .. " without an active queue.\n")
		   end
		end
	     end
	  end -- do DEALLOCATE QUEUES

	  do -- CHANGE RATES
	     -- change rates of active flows if it's time
	     local now = dpdk.getTime()
	     for flowId, queueNo in pairs(queues) do
		assert(queueRates[queueNo].valid)
		if queueRates[queueNo].changeTime ~= -1
		and queueRates[queueNo].changeTime <= now then
		   queueRates[queueNo].currentRate
		      = queueRates[queueNo].nextRate
		   queueRates[queueNo].nextRate = -1
		   queueRates[queueNo].changeTime = -1

		   local configuredRate = queueRates[queueNo].currentRate
		   assert(configuredRate <= perc_constants.END_HOST_LINK_MBPS)
		   local dTxQueue = dev:getTxQueue(queueNo)
		   dTxQueue:setRate(configuredRate)
		end -- if rateInfo[0].changeTime ~= nil ..
	     end -- for queueNo, ..
	  end

      end -- while dpdk.running()	
      dpdk.sleepMillis(5000)
end


return controlMod
