# perc-moongen

## Setup
- Clone https://github.com/lavanyaj/MoonGen.git
- Extract to MoonGen/examples as perc-moongen
- Run MoonGen/examples/perc-moongen/run_perc.py to run PERC using default options (one sender to one receiver, original DCTCP flow sizes, 1000 flows)

## Parameters
All the *tunable parameters* are in perc-moongen/constants-han1.lua. Defaults are
- *Start Rate* : 12 Mb/s when any flow starts
- *Tx Ack Timeout Interval*: 5s Will time out/ reclaim resources from flows that haven't finished in this time
- *Rtts*: Flows wait for 2 * *Rtts* before increasing their rates to make sure everyone's gotten the memo, and required flows have reduced their rates
- *Max Queues*: number of queues to use for sending flows (should be an upper bound on number of active flows we expect, 40-50 for medium load)
- *End Host Link Mb/s*: bandwidth reserved for PERC to allocate to flows (remaining used for control, ACKs, low rate flows etc.)

*Sizes of packets/ payload*. Note that there is some control info in the control packet payload (queue #) 
and the data packet payload (size, sequence number)(ideally these should be in a header)

*Reserved Queues*: for receiving ACKs, control packets. for sending ACKs, control packets, new control packets, packets to be dropped.

*MAC address to dev  mapping*: where dev is the id of the device that's configured when you say dpdk.configure for port = dev.
