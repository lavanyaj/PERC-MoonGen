-- Usage:
-- sudo ./build/MoonGen examples/perc-moongen-single/main.lua 0 1 examples/perc-moongen/DCTCP_CDF 1000
-- one sending thread for ort 0, one receiving thread for port 1
local filter    = require "filter"
local dpdk	= require "dpdk"
local device	= require "device"
local log = require "log"
local pipe		= require "pipe"
local eth = require "proto.ethernet"

local perc_constants = require "examples.perc-moongen-single.constants"
local monitor = require "examples.perc-moongen-single.monitor"
local ipc = require "examples.perc-moongen-single.ipc"
local fsd = require "examples.perc-moongen-single.flow-size-distribution"

local app = require "examples.perc-moongen-single.app"
local control = require "examples.perc-moongen-single.control1"
local data = require "examples.perc-moongen-single.data"


function master(txPort, rxPort, cdfFilepath, numFlows)
   if not txPort or not rxPort or not cdfFilepath or not numFlows then
      return log:info("usage: txPort rxPort cdfFilepath numFlows")
   end
   

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
   rxDev:l2Filter(eth.TYPE_DROP, perc_constants.DROP_QUEUE)
   txDev:l2Filter(eth.TYPE_DROP, perc_constants.DROP_QUEUE)
   

   dpdk.setRuntime(5)
   local txIpcPipes = ipc.getInterVmPipes()
   local rxIpcPipes = ipc.getInterVmPipes()
   local monitorPipes = monitor.getPerVmPipes({txPort, rxPort})
   local readyPipes = ipc.getReadyPipes(2)
   local tableDst = {}
   tableDst[txPort] = txPort
   tableDst[rxPort] = rxPort
   
   dpdk.launchLua("sendDataSlave", txDev, cdfFilepath,
		  numFlows, txPort, txPort,
		  tableDst, readyPipes, 1)
   dpdk.launchLua("recvDataSlave", rxDev, nil, nil,
		  readyPipes, 2)
   dpdk.waitForSlaves()
end

function sendDataSlave(dev, cdfFilepath, numFlows,
		       percgSrc, ethSrc, tableDst,
		       readyPipes, id)
   local readyInfo = {["pipes"]=readyPipes, ["id"]=id}
   print(readyInfo.id)
   data.txSlave(dev, cdfFilepath, numFlows,
		percgSrc, ethSrc, tableDst,
		readyInfo)		
end

function recvDataSlave(dev, ipcPipes, monitorPipes, readyPipes, id)
   local readyInfo = {["pipes"]=readyPipes, ["id"]=id}
   print(readyInfo.id)

   data.rxSlave(dev, readyInfo)

end

