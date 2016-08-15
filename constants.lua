constantsMod = {
   ["APP_MAX_FLOW_SIZE_BYTES"] = 3e6,
   ["APP_INTERARRIVAL_TIME_S"] = 0.1,
   ["CONTROL_PACKET_SIZE"] = 128,
   -- 11B b/n control and host state, 6 b/n .. agg 80
   ["DATA_PACKET_SIZE"] = 1500,
   ["DATA_PACKET_PAYLOAD"] = 1200, -- actually check   
   ["DATA_SEND_TXMEMPOOL_SIZE"] = nil,
   ["DATA_RECV_TXMEMPOOL_SIZE"] = nil,
   ["ACK_PACKET_SIZE"] = 256,
   ["DATA_RXQUEUE"] = 0,
   ["CONTROL_TXQUEUE"] = 1,
   ["CONTROL_RXQUEUE"] = 1,
   ["ACK_RXQUEUE"] = 2,
   ["ACK_TXQUEUE"] = 2,
   ["DROP_QUEUE"] = 3,
   ["ETHTYPE_ACK"] = 5678,
   ["ETHTYPE_DATA"] = 6789,
   ["MAX_QUEUES"] = 20,
   ["tx_ack_timeout"] = 0.1,
   ["rx_ack_timeout"] = 0.05,
   ["LOG_EVERY_N_SECONDS"] = 1e-3,
   ["NEW_FLOWS_PER_CONTROL_LOOP"] = 2,
   ["NIC_DESCRIPTORS_PER_QUEUE"] = 512, -- not 40
   ["END_HOST_LINK_MBPS"] = 8000,
   ["WARN_DATA"] = true,
   ["LOG_RXDATA"] = false,
   ["LOG_TXDATA"] = false,
   ["LOG_CONTROL"] = false,
   ["WARN_CONTROL"] = true,
   ["LOG_APP"] = false
}

return constantsMod
