-- Usage:
-- sudo ./build/MoonGen examples/perc-moongen/main.lua 0 1 examples/perc-moongen/DCTCP_CDF 1000
-- two threads per port
local filter    = require "filter"
local dpdk	= require "dpdk"
local device	= require "device"
local log = require "log"
local pipe		= require "pipe"
local eth = require "proto.ethernet"

local perc_constants = require "examples.perc-moongen.constants-han1"
local ipc = require "examples.perc-moongen.ipc"
local fsd = require "examples.perc-moongen.flow-size-distribution"

local data = require "examples.perc-moongen.data1"


function master(mode, cdfFilepath, scaling, interArrivalTime, numFlows)
   if not mode or not cdfFilepath or not scaling  or not interArrivalTime or not numFlows then
      return log:info("usage: single/multi cdfFilepath scaling numFlows\n"
			 .. " multi -> port 0 and 1 sending to 2 and 3"
			 .. ", single -> port 0 sending to port 1\n"
			 .. " scaling = 0.1 -> flows are ten times shorter")
   end
   
   if mode == "single" then singleConnection(cdfFilepath, scaling, interArrivalTime, numFlows)
   else multiConnection(cdfFilepath, scaling, numFlows) end
end

function singleConnection(cdfFilepath, scaling, interArrivalTime, numFlows)
   local txPort = 0
   local rxPort = 1
   -- local macAddrType = ffi.typeof("union mac_address")	
   local txDev = device.config{port = txPort,
			       rxQueues = 5,
			       txQueues = perc_constants.MAX_QUEUES+1}
   local rxDev = device.config{port = rxPort,
			       rxQueues = 5,
			       txQueues = perc_constants.MAX_QUEUES+1}
   
	-- filters for data packets
   txDev:l2Filter(eth.TYPE_ACK, perc_constants.ACK_RXQUEUE)
   rxDev:l2Filter(eth.TYPE_ACK, perc_constants.ACK_RXQUEUE)
   
   -- filters for control packets
   txDev:l2Filter(eth.TYPE_PERCG, perc_constants.CONTROL_RXQUEUE)
   rxDev:l2Filter(eth.TYPE_PERCG, perc_constants.CONTROL_RXQUEUE)
   log:info("filter packets with ethType "
	       .. eth.TYPE_PERCG .. " to rx queue "
	       .. perc_constants.CONTROL_RXQUEUE .. " on device " .. txDev.id
	       .. ", " .. eth.TYPE_PERCG .. " to rx queue "
	       .. perc_constants.CONTROL_RXQUEUE .. " on device " .. rxDev.id)

   
   rxDev:l2Filter(eth.TYPE_DROP, perc_constants.DROP_QUEUE)
   txDev:l2Filter(eth.TYPE_DROP, perc_constants.DROP_QUEUE)

   -- rate limit control packets
   txDev:getTxQueue(perc_constants.CONTROL_TXQUEUE):setRate(50)
   rxDev:getTxQueue(perc_constants.CONTROL_TXQUEUE):setRate(50)

   dpdk.setRuntime(5)
   local readyPipes = ipc.getReadyPipes(2)
   local tableDst = {}
   tableDst[txPort] = txPort
   tableDst[rxPort] = rxPort
   
   dpdk.launchLua("loadDataSlave", txDev, cdfFilepath,
		  scaling, interArrivalTime, numFlows, txPort, txPort,
		  tableDst,
		  true, false,
		  readyPipes, 1)

   -- dpdk.launchLua("loadDataSlave", rxDev, cdfFilepath,
   -- 		  numFlows, rxPort, rxPort,
   -- 		  tableDst,
   -- 		  true, false,
   -- 		  readyPipes, 2)

   dpdk.launchLua("loadDataSlave", rxDev, nil,
   		  nil, nil, nil, rxPort, rxPort,
   		  nil,
   		  false, true,
   		  readyPipes, 2)

   -- dpdk.launchLua("loadDataSlave", rxDev, nil,
   -- 		  nil, rxPort, rxPort,
   -- 		  nil,
   -- 		  false, true,
   -- 		  readyPipes, 2)

   dpdk.waitForSlaves()
end

function loadDataSlave(dev, cdfFilepath, 
		       scaling, interArrivalTime, numFlows,
		       percgSrc, ethSrc, tableDst,
		       isSending, isReceiving,
		       readyPipes, id)
   local readyInfo = {["pipes"]=readyPipes, ["id"]=id}
   print(readyInfo.id)
   if tableDst ~= nil and percgSrc ~= nil then   
      tableDst[percgSrc] = nil end
   data.dataSlave(dev, cdfFilepath, scaling, interArrivalTime, numFlows,
		percgSrc, ethSrc, tableDst,
		isSending, isReceiving,
		readyInfo)		
end

