local ffi = require("ffi")
local pipe		= require "pipe"
local headers = require "headers"
local utils = require "utils"
local dpdkc = require "dpdkc"

ipcMod = {}

ffi.cdef[[
// application to control
// start a flow of size packets at time
typedef struct { int flow, size, valid;
uint8_t percgSrc;
uint8_t percgDst;
union mac_address ethSrc;
union mac_address ethDst;
} facStartMsg;  
typedef facStartMsg* pFacStartMsg;

// control to data
// start sending data for flow on queue
typedef struct { int flow,  size, queue, valid;
uint8_t percgSrc;
uint8_t percgDst;
union mac_address ethSrc;
union mac_address ethDst;
} fcdStartMsg;  
typedef fcdStartMsg* pFcdStartMsg;

// data to control
// finished sending packets for flow at time
typedef struct { int flow; double endTime, valid;} fdcFinMsg;  
typedef fdcFinMsg* pFdcFinMsg;
// got data FIN ACK for flow at time
typedef struct { int flow, size; double endTime, valid;} fdcFinAckMsg;
typedef fdcFinAckMsg* pFdcFinAckMsg;

// resource exhausted, can't start flow
typedef struct { int flow; double valid;} fcaResourceExhaustedMsg;  
typedef fcaResourceExhaustedMsg* pFcaResourceExhaustedMsg;

// finished sending packets for flow at time
typedef struct { int flow; double endTime, valid;} fcaFinMsg;  
typedef fcaFinMsg* pFcaFinMsg;
// got data FIN ACK for flow at time
typedef struct { int flow, size; double endTime, valid;} fcaFinAckMsg;
typedef fcaFinAckMsg* pFcaFinAckMsg;
]]

local macAddrType = ffi.typeof("union mac_address")
local istype = ffi.istype

function ipcMod.sendFacStartMsg(pipes, flow, size,
				percSrc, percDst, ethSrc, ethDst)
   local msg = ffi.new("facStartMsg")
   msg.flow = flow
   msg.size = size
   msg.valid = 1234
   
   msg.percgSrc = percSrc
   msg.percgDst = percDst

   if type(ethSrc) == "number" then
      local buf = ffi.new("char[20]")
      dpdkc.get_mac_addr(ethSrc, buf)
      local ethSrcStr = ffi.string(buf)      
      msg.ethSrc = parseMacAddress(ethSrcStr)
   elseif istype(macAddrType, ethSrc) then
      msg.ethSrc = ethSrc
   else
      assert(false)
   end

   if type(ethDst) == "number" then
      local buf = ffi.new("char[20]")
      dpdkc.get_mac_addr(ethDst, buf)
      local ethDstStr = ffi.string(buf)      
      msg.ethDst = parseMacAddress(ethDstStr)
   elseif istype(macAddrType, ethDst) then
      msg.ethDst = ethDst
   else
      assert(false)
   end

   pipes["fastPipeAppToControlStart"]:send(msg)
   --return msg
end

function ipcMod.sendFcdStartMsg(pipes, flow, size, queue,
			       percSrc, percDst, ethSrc, ethDst)

   assert(istype(macAddrType, ethSrc))
   assert(istype(macAddrType, ethDst))
   
   local msg = ffi.new("fcdStartMsg")
   msg.flow = flow
   msg.size = size
   msg.queue = queue
   msg.valid = 1234
   msg.percgSrc = percSrc
   msg.percgDst = percDst
   msg.ethSrc = ethSrc
   msg.ethDst = ethDst
   pipes["fastPipeControlToDataStart"]:send(msg)
   --return msg
end

function ipcMod.sendFdcFinMsg(pipes, flow, endTime)
   local msg = ffi.new("fdcFinMsg")
   msg.flow = flow
   msg.endTime = endTime
   msg.valid = 1234
   pipes["fastPipeDataToControlFin"]:send(msg)
   --return msg
end

function ipcMod.sendFcaFinMsg(pipes, flow, endTime)
   local msg = ffi.new("fcaFinMsg")
   msg.flow = flow
   msg.endTime = endTime
   msg.valid = 1234
   pipes["fastPipeControlToAppFin"]:send(msg)
   --return msg
end

function ipcMod.sendFdcFinAckMsg(pipes, flow, received, endTime)
   local msg = ffi.new("fdcFinAckMsg")
   msg.flow = flow
   msg.size = received
   msg.endTime = endTime
   msg.valid = 1234
   pipes["fastPipeDataToControlFinAck"]:send(msg)
   --return msg
end

function ipcMod.sendFcaResourceExhaustedMsg(pipes, flow)
   local msg = ffi.new("fcaResourceExhaustedMsg")
   msg.flow = flow
   msg.valid = 1234
   pipes["fastPipeControlToAppResourceExhausted"]:send(msg)
end

function ipcMod.sendFcaFinAckMsg(pipes, flow, received, endTime)
   local msg = ffi.new("fcaFinAckMsg")
   msg.flow = flow
   msg.size = received
   msg.endTime = endTime
   msg.valid = 1234
   pipes["fastPipeControlToAppFinAck"]:send(msg)
   --return msg
end

function ipcMod.acceptFacStartMsgs(pipes)
   return ipcMod.fastAcceptMsgs(pipes, "fastPipeAppToControlStart", "pFacStartMsg", 20)
end

function ipcMod.acceptFcdStartMsgs(pipes)
   return ipcMod.fastAcceptMsgs(pipes, "fastPipeControlToDataStart", "pFcdStartMsg", 20)
end

function ipcMod.acceptFdcFinMsgs(pipes)
   return ipcMod.fastAcceptMsgs(pipes, "fastPipeDataToControlFin", "pFdcFinMsg", 20)
end

function ipcMod.acceptFcaResourceExhaustedMsgs(pipes)
   return ipcMod.fastAcceptMsgs(pipes, "fastPipeControlToAppResourceExhausted", "pFcaResourceExhaustedMsg", 20)
end

function ipcMod.acceptFcaFinMsgs(pipes)
   return ipcMod.fastAcceptMsgs(pipes, "fastPipeControlToAppFin", "pFcaFinMsg", 20)
end

function ipcMod.acceptFdcFinAckMsgs(pipes)
   return ipcMod.fastAcceptMsgs(pipes, "fastPipeDataToControlFinAck", "pFdcFinAckMsg", 20)
end

function ipcMod.acceptFcaFinAckMsgs(pipes)
   return ipcMod.fastAcceptMsgs(pipes, "fastPipeControlToAppFinAck", "pFcaFinAckMsg", 20)
end

function ipcMod.sendMsgs(pipes, pipeName, msg)	 
	 -- update send time for this pipe in msg.flowEvent.
	 -- and can turn off logging
   pipes[pipeName]:send(msg)
end

function ipcMod.acceptMsgs(pipes, pipeName, msgType)
   if pipes == nil or pipes[pipeName] == nil then
      print("acceptMsgs on nil pipe! return!!")
      return
   end 

   local pipe = pipes[pipeName]
   local numMsgs = pipe:count()
   if numMsgs ~= 0 then
      print(tostring(numMsgs) .. " msgs on pipe " .. pipeName)	
   end
   local msgs = {}
   while numMsgs > 0 do
      local msg = pipe:recv()
      if msgType ~= nil then
	 msg = ffi.cast(msgType, msg)
      end
      msgs[numMsgs] = msg
      local flowId = msg.flow
      if msgType ~= nil then flowId = msg[0].flow end
      --print("Got msg # " .. tostring(numMsgs) ..
      --	    " for flow " .. tostring(flowId)
      --	       .. " on pipe " .. pipeName)
      numMsgs = numMsgs - 1
   end
   return msgs
end	 

-- try to get as many packets as possible in waitxTenUs x 10us
function ipcMod.fastAcceptMsgs(pipes, pipeName, msgType, waitxTenUs)
   if pipes == nil then
      return {}
   end

   if pipes[pipeName] == nil then 
      print("acceptMsgs on nil pipe! return!!")
      return {}
   end
   
   local pipe = pipes[pipeName]
   local msgs = {}
   
   --local numMsgs = pipe:count()
   --if numMsgs ~= 0 then
   --   print(tostring(numMsgs) .. " msgs on pipe " .. pipeName)	
   --end

   local numMsgs = 1
   assert(msgType ~= nil)
   assert(waitxTenUs > 0)
   while waitxTenUs > 0 and numMsgs < 2 do
      local msg = pipe:tryRecv(1)
      if (msg ~= nil) then
	 msg = ffi.cast(msgType, msg)
	 if (msg.valid == 1234) then
	    msgs[numMsgs] = msg
	    --print("Got msg # " .. tostring(numMsgs) ..
	    --	  " for flow " .. tostring(msg.flow)
	    --	     .. " on pipe " .. pipeName)
	    numMsgs = numMsgs + 1
	 else
	    --print("Got invalid msg with valid = " ..
	    --	     msg.valid
	    --	     .. " on pipe " .. pipeName)
	    
	 end
      end
      waitxTenUs = waitxTenUs - 1
   end
   return msgs
end	 

function ipcMod.getInterVmPipes()
   -- controlToData: to start flow
   -- dataToControl: end of data
   -- appToControl: to start flow
   -- appToData: to end flow
   -- slowPipe ApplicationSlave: (t, start/ change rate/ end of data/ end of control) 
	 local pipes =  {
	    ["fastPipeAppToControlStart"] = pipe.newFastPipe(20),
	    ["fastPipeControlToDataStart"] = pipe.newFastPipe(20), 
	    ["fastPipeDataToControlFin"] = pipe.newFastPipe(20),
	    ["fastPipeDataToControlFinAck"] = pipe.newFastPipe(20),
	    ["fastPipeControlToAppFin"] = pipe.newFastPipe(20),
	    ["fastPipeControlToAppFinAck"] = pipe.newFastPipe(20),
	    ["fastPipeControlToAppResourceExhausted"] = pipe.newFastPipe(20),
	    ["slowPipeControlToApp"] = pipe.newSlowPipe()
	 }
	 return pipes
end

function ipcMod.getReadyPipes(numParticipants)
	 -- Setup pipes that slaves use to figure out when all are ready
	 local readyPipes = {}
	 local i = 1
	 while i <= numParticipants do
	       readyPipes[i] = pipe.newSlowPipe()
	       i = i + 1
	       end
	 return readyPipes
end

function ipcMod.waitTillReady(readyInfo)
	 -- tell others we're ready and check if others are ready
   local myPipe = readyInfo.pipes[readyInfo.id]
   if myPipe ~= nil then	 	 
      -- tell others I'm ready  
      for pipeNum,pipe in ipairs(readyInfo.pipes) do
	 if pipeNum ~= readyInfo.id then 
	    pipe:send({["1"]=pipeNum})
	 end
	 pipeNum = pipeNum + 1
      end
	
      local numPipes = table.getn(readyInfo.pipes)
      
      -- busy wait till others are ready
      local numReadyMsgs = 0	 
      while numReadyMsgs < numPipes-1 do
	 if myPipe:recv() ~= nil then 
	    numReadyMsgs = numReadyMsgs + 1
	    --print("Received " .. numReadyMsgs .. " ready messages on pipe # " .. readyInfo.id)
	 end
      end
      
      --print("Received " .. numReadyMsgs .. " ready messages on pipe # " .. readyInfo.id)
   end
end

return ipcMod
