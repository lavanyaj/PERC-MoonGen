constantsMod = {
   ["APP_MAX_FLOW_SIZE_BYTES"] = 3e6,
   ["CONTROL_PACKET_SIZE"] = 128,
   -- 11B b/n control and host state, 6 b/n .. agg 80
   ["DATA_PACKET_SIZE"] = 1500,
   ["DATA_PACKET_PAYLOAD"] = 1200, -- actually check   
   ["DATA_SEND_TXMEMPOOL_SIZE"] = nil,
   ["DATA_RECV_TXMEMPOOL_SIZE"] = nil,
   ["ACK_PACKET_SIZE"] = 256,
   ["DATA_RXQUEUE"] = 0,
   ["CONTROL_TXQUEUE"] = 1,
   ["NEW_CONTROL_TXQUEUE"] = 2,
   ["CONTROL_RXQUEUE"] = 1,
   ["ACK_RXQUEUE"] = 3,
   ["ACK_TXQUEUE"] = 3,
   ["DROP_QUEUE"] = 4,
   ["MAX_QUEUES"] = 20,
   ["MAX_SENDERS"] = 1,
   ["tx_ack_timeout"] = 100,
   ["rtts"] = 0.0005, -- delay before increasing rate
   ["startRate"] = 11, -- Mb/s
   -- device id to mac address mapping (based on FPGA routing
   -- and wiring as on 8/18 on DCA server)
   ["ethAddrStr2"] = "0c:33:33:33:33:33",
   ["ethAddrStr3"] = "0c:44:44:44:44:44",
   ["ethAddrStr0"] = "0c:11:11:11:11:11",
   ["ethAddrStr1"] = "0c:22:22:22:22:22",
   ["END_HOST_LINK_MBPS"] = 8000,
}

return constantsMod
