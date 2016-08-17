-- With main1.lua 1000 flows
--
-- Device: id=0] Received 0.229732 (StdDev 0.000809) Mpps, 2744.070666 (StdDev 10.979793) MBit/s, 2780.827797 (StdDev 11.109279) MBit/s wire rate on average.
--[Device: id=1] Received 0.172925 (StdDev 0.001342) Mpps, 2053.154747 (StdDev 17.118611) MBit/s, 2080.822797 (StdDev 17.333388) MBit/s wire rate on average.
--
--

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
   ["NEW_CONTROL_TXQUEUE"] = 2,
   ["CONTROL_RXQUEUE"] = 1,
   ["ACK_RXQUEUE"] = 3,
   ["ACK_TXQUEUE"] = 3,
   ["DROP_QUEUE"] = 4,
   ["MAX_QUEUES"] = 60,
   ["inter_arrival_time"] = 0.1 * (0.01),
   ["tx_ack_timeout"] = 2 * (0.1),
   ["rx_ack_timeout"] = 4 * (0.05),
   ["rtts"] = 0.0005,
   ["LOG_EVERY_N_SECONDS"] = 1e-3,
   ["NEW_FLOWS_PER_CONTROL_LOOP"] = 2,
   ["NIC_DESCRIPTORS_PER_QUEUE"] = 512, -- not 40
   ["END_HOST_LINK_MBPS"] = 8000,
   ["WARN_DATA"] = true,
   ["LOG_RXDATA"] = false,
   ["LOG_TXDATA"] = false,
   ["LOG_CONTROL"] = true,
   ["WARN_CONTROL"] = true,
   ["LOG_APP"] = true
}

return constantsMod
