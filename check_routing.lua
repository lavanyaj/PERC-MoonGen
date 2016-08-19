--- Duplicate packet detection, requires two directly connected ports
local mg		= require "dpdk"
local memory	= require "memory"
local device	= require "device"
local stats		= require "stats"
local log		= require "log"

--local PKT_SIZE	= 60
local PKT_SIZE	= 750
--local PKT_SIZE	= 1500

local ethAddr = {
   ["0"] = "33:33:33:33:33:0c",
   ["1"] = "44:44:44:44:44:0c",
   ["2"] = "0c:11:11:11:11:11",
   ["3"] = "0c:22:22:22:22:22",
}

--sudo build/MoonGen examples/*single/check_routing.lua 0 "0c:11:11:11:11:11"
function master(txPort, ethDstAddr)
   if not txPort or not ethDstAddr then
      return log:info("usage: txPort ethDstAddr")
   end

   local rxDevs = {}
   for i = 0, 3 do
      rxDevs[i] = device.config{port = i, rxQueues = 5}
   end
   local txDev = rxDevs[txPort]   
    rxDevs[2]:l2Filter(0x0708, 3)
    rxDevs[3]:l2Filter(0x0708, 3)

   mg.launchLua("txSlave", txDev, ethDstAddr)

   --mg.launchLua("rxSlave", rxDevs[0]:getRxQueue(0))
   --mg.launchLua("rxSlave", rxDevs[1]:getRxQueue(0))
   mg.launchLua("rxSlave", rxDevs[2]:getRxQueue(3))
   mg.launchLua("rxSlave", rxDevs[3]:getRxQueue(3))
   
   mg.waitForSlaves()
end

function txSlave(dev, ethDstAddr)
   
   mg.sleepMillis(500) -- wait a few milliseconds to ensure that the rx thread is running
   local mem = memory.createMemPool(function(buf)
	 buf:getUdpPacket():fill{
	    pktLength = PKT_SIZE,
	    ethSrc = dev,
	    ethDst = ethDstAddr,
				}
   end)
   local bufs = mem:bufArray()
   local ctrName = "Port_" .. tostring(dev.id)

   local queue = dev:getTxQueue(0)
   while mg.running() do
      bufs:alloc(PKT_SIZE)
      for _, buf in ipairs(bufs) do
	 local pkt = buf:getUdpPacket()
	 pkt.eth:setDstString(ethDstAddr)
	 pkt.eth:setType(0x0708)
	 --assert(pkt.eth:getDst() == ethDstAddr)
      end

      queue:send(bufs)
   end   

end

function rxSlave(queue)
        local bufs = memory.bufArray()

	local ctrName = "Port_" .. tostring(queue.id)

	local recvd = false
	while mg.running() and recvd == false do
	   local rx = queue:recv(bufs)
	   for i = 1, rx do
	      local buf = bufs[i]
	      local pkt = buf:getUdpPacket()
	      if i == 1 then
		 log:info(ctrName .. " received packet from "
			     .. pkt.eth:getSrcString() .. " to "
			     .. pkt.eth:getDstString() .. " of type "
			     .. pkt.eth:getType())
		 recvd = true
	      end
	   end

	   bufs:freeAll()
	end

end


