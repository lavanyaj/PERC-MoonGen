local ffi = require("ffi")
local dpdk	= require "dpdk"
local utils = require "utils"

local ipc = require "examples.perc-moongen.ipc"
local monitor = require "examples.perc-moongen.monitor"
local perc_constants = require "examples.perc-moongen.constants"
local fsd = require "examples.perc-moongen.flow-size-distribution"

appMod = {}
local PKT_PAYLOAD_SIZE = perc_constants.DATA_PACKET_PAYLOAD
local MAX_FLOW_SIZE = perc_constants.APP_MAX_FLOW_SIZE_BYTES
--(2000 * 1500) -- 15000000 --15000000
local MEAN_INTER_ARRIVAL_TIME = perc_constants.APP_INTERARRIVAL_TIME_S

function appMod.log(str)
   if perc_constants.LOG_APP then
      print("app2.lua log: " .. str)
   end
end

function appMod.bytesToPackets(numBytes)
   return math.ceil(numBytes / PKT_PAYLOAD_SIZE)
end

-- in main loop
ffi.cdef[[
typedef struct {
 int flowId;
 int totalBytes;
 int totalPackets;
 double startTime;
 double ackedTime;
 int ackedBytes;
 int ackedPackets;
 int flowSizeBin;
} flowInfo;
]]

function appMod.bytesToSizeStr(numBytes)
   if numBytes <= 1e4 then
      return "small"
   elseif numBytes <= 1e6 then
      return "medium"
   else
      return "large"
   end
end

function appMod.finalize(flowInfoTable, numFlows, linkSpeed, fileName)
   local now = dpdk.getTime()
   local numFlowsBySize = {}
   local numFinishedBySize = {}
   fctFilesBySize = {}
   for _, sizeStr in ipairs({"small", "medium", "large"}) do
      numFlowsBySize[sizeStr] = 0
      numFinishedBySize[sizeStr] = 0
      fctFilesBySize[sizeStr] = io.open(fileName .. "-" .. sizeStr .. "-" .. tostring(now) .. ".txt")
   end   
   local fctFile = io.open(fileName .. "-" .. tostring(now))
   assert(fctFile ~= nil)
   fctFile:write(string.format("flow finished linkSpeed totalBytes totalPackets fct startTime endTime minFctAt1G minFctAt10G normFct ackedPackets"))
   for i = 0, numFlows-1 do
      local flowInfo = flowInfoTable[i]
      if flowInfo.startTime > 0 then
	 local fct = (flowInfo.ackedTime - flowInfo.startTime) * 1e6 -- microseconds
	 local finishedStr = "finished"
	 if (flowInfo.totalPackets ~= flowInfo.ackedPackets) then
	    finishedStr = "unfinished"
	 end
	 
	 local minFctAt10G = flowInfo.totalPackets * 1.2
	 local minFctAt1G = flowInfo.totalPackets * 1.2 * 10
	 local normFct = fct/minFctAt10G	 
	 if linkSpeed == 1000 then normFct = fct/minFctAt1G end -- link speeds in Mb/s
	 local sizeStr = appMod.bytesToSizeStr(totalBytes)
	 numFlowsBySize[sizeStr] = numFlowsbySize[sizeStr] + 1
	 if finishedStr == "finished" then
	    numFinishedBySize[sizeStr] = numFinishedbySize[sizeStr] + 1
	    fctFilesBySize:write(normFct .. "\n")
	 end
	 fctFile:write(
	    tostring(i) .. " " .. tostring(finishedStr) .. " " .. tostring(linkSpeed)
	       .. " " .. tostring(flowInfo.totalBytes) .. " " .. tostring(flowInfo.totalPackets)
	       .. " " .. tostring(fct) .. " " .. tostring((flowInfo.startTime * 1e6))
	       .. " " .. tostring(flowInfo.ackedTime * 1e6) .. " " .. tostring( minFctAt1G)
		     .. " " .. tostring(minFctAt10G) .. " " .. tostring(normFct)
	       .. " " .. tostring(flowInfo.ackedPackets) .. "\n")
	 end
   end
   -- then I would os.execute( R script to get 99th and median) to capture 99th and median for all sizes
   -- then I would output it the the GUI's input file
end

function appMod.applicationSlave(pipes, cdfFilepath,
				 percgSrc, ethSrc,
				 tableDst, readyInfo, monitorPipe)
   local thisCore = dpdk.getCore()
   appMod.log("Running application slave on core " .. thisCore)

   local flowSizes = fsd.create()
   flowSizes:loadCDF(cdfFilepath)
   local avgFlowSize = flowSizes:avg()
   assert(avgFlowSize > 0)
   appMod.log("loaded flow sizes file with avg. flow size "
     	      .. tostring(avgFlowSize/1500) .. " packets.\n")

   local fixedPercgDst, fixedEthDst = next(tableDst)
   assert(fixedPercgDst ~= nil)
   assert(fixedEthDst ~= nil)

   appMod.log("sending all flows to percgDst " .. fixedPercgDst
		 .. " and ethDst " .. fixedEthDst)     	      
   
   local numFlows = 10
   local source = 0xaa
   local destination = 0xff
   local meanInterArrivalTimeS = 1 --3e-3
   
   ipc.waitTillReady(readyInfo)

   local now = dpdk.getTime()
   local exitTime = now + 600
   
   local nextSendTime = now
   local startFlowId = 100
   local nextFlowId = startFlowId

   local flowInfoTable = ffi.new("flowInfo[?]", nextFlowId + numFlows)
   local numFinished = 0

   -- TODO(lav): maybe will need a backlog of flows that weren't allowed to start

   local numBacklogged = 0
   local backlog = {}
   local done = false
   local lastFlowId = numFlows + startFlowId - 1
   while dpdk.running() and  now < exitTime do
      now = dpdk.getTime()
      -- Get resource exhausted msgs and add flows to backlog to try again
      do
	 local msgs = ipc.fastAcceptMsgs(
	    pipes,
	    "fastPipeControlToAppResourceExhausted",
	    "pFcaResourceExhaustedMsg", 20)	 
	 if next(msgs) ~= nil then
	    for msgNo, msg in pairs(msgs) do
	       if backlog[msg.flow] == false then
		  numBacklogged = numBacklogged + 1
		  backlog[msg.flow] = true
	       end		     
	    end
	 end
      end -- ends do
      
      -- Get acks from PERC and update committed packets for all flows
      local msgs = ipc.fastAcceptMsgs(pipes, "fastPipeControlToAppFinAck", "pFcaFinAckMsg", 20)
      if next(msgs) ~= nil then

	 for msgNo, msg in pairs(msgs) do
	    local lastAckedTime = flowInfoTable[msg.flow].ackedTime
	    local lastAckedPackets = flowInfoTable[msg.flow].ackedPackets
	    local totalPackets = flowInfoTable[msg.flow].totalPackets
	    assert(msg.flow < nextFlowId) -- sanity check
	    assert(lastAckedTime > 0 and lastAckedTime < now)
	    local ackedPackets = msg.size
	    local ackedTime = now
	    if ackedPackets > lastAckedPackets then
	       flowInfoTable[msg.flow].ackedTime = ackedTime
	       flowInfoTable[msg.flow].ackedPackets = ackedPackets
	       if ackedPackets == totalPackets then
		  appMod.log("Flow " .. msg.flow .. " finished.\n")
		  numFinished = numFinished + 1
	       end
	    end
	 end
      end

      -- Start new flows if it's time
      do
	 local now = dpdk.getTime()
	 if now > nextSendTime and nextFlowId <= lastFlowId
	 and numBacklogged == 0 then
	    local numBytes = flowSizes:value()
	    local numPackets = appMod.bytesToPackets(numBytes)
	    flowInfoTable[nextFlowId].startTime = now
	    flowInfoTable[nextFlowId].totalBytes = numBytes
	    flowInfoTable[nextFlowId].totalPackets = numPackets
	    flowInfoTable[nextFlowId].ackedTime = now
	    flowInfoTable[nextFlowId].ackedPackets = 0

	    ipc.sendFacStartMsg(pipes, nextFlowId, numPackets,
				percgSrc, fixedPercgDst,
				ethSrc, fixedEthDst)
	 
	    nextFlowId = nextFlowId+1
	    nextSendTime = now + poissonDelay(meanInterArrivalTimeS)
	    -- don't want to stall
	    local tries = 0
	    while (nextSendTime - now > 5) do
	       nextSendTime = now + poissonDelay(meanInterArrivalTimeS)
	       tries = tries + 1
	       assert(tries < 50)
	    end
	    
	    -- appMod.log("Memory in use " .. collectgarbage("count") .. "Kb")
	    appMod.log("Change  at "
			  .. now
			  .. ": added "
			  .. (nextFlowId-1) .. " of size " .. numBytes
			  .. "B, so "
			  .. (nextFlowId - numFinished - startFlowId)
			  .. " active flows"
			  .. ", nextFlowId is "
			  .. nextFlowId
			  .. ", stop when nextFlowId is "
			  .. lastFlowId + 1
			  .. ", next sendTime in "
			  .. (nextSendTime - now) .. "s.\n")

	    if (nextFlowId == lastFlowId + 1) then
	       appMod.log("app exiting in 2s after " .. numFlows .. "\n")
	       exitTime = now + 2
	    end
	 elseif now > nextSendTime and numBacklogged > 0 then	    
	    local backloggedFlowId, _ = next(backlog)
	    appMod.log("app has " .. numBacklogged .. " backlogged flows"
		       .. ", resending start request for "
			  .. backloggedFlowId)
	    backlog[backloggedFlowId] = nil
	    numBacklogged = numBacklogged - 1
	    ipc.sendFacStartMsg(pipes, backloggedFlowId, numPackets,
	    			percgSrc, fixedPercgDst,
				ethSrc, fixedEthDst)	
	    nextSendTime = now + poissonDelay(meanInterArrivalTimeS)

	    -- don't want to stall
	    local tries = 0
	    while (nextSendTime - now > 5) do
	       nextSendTime = now + poissonDelay(meanInterArrivalTimeS)
	       tries = tries + 1
	       assert(tries < 50)
	    end
	 else
	 end
      end
   end
   appMod.finalize(flowInfoTable, numFlows, 10000, "output-app".. readyInfo.id .."-" .. source .. "-" .. destination)
end

return appMod
