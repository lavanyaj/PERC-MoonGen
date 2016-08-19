local ffi = require("ffi")
local pipe		= require "pipe"
local headers = require "headers"
local utils = require "utils"
local dpdkc = require "dpdkc"

ipcMod = {}

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
