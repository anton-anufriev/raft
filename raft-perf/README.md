## raft performance test

1. Setup JAVA_HOME in all environments

export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home

2. Startup mediadriver in all environments (one on each box, or just one if all instances run on the same box)

./bin/media-driver low-latency.properties

3. Start raft-perf instances:

args:
    serverId - Id of the server instance [0 .. clusterSize-1]
    clusterSize - number of all instances in the cluster
    messagesToSend - command messages to send to raft
    warmupMessages - number of first messages to ignore when recording latency metric
    memoryMappingType -  SYNC (in the same thread),  ASYNC (in a separate thread, requires extra thread per raft instance)
    serverChannel - multicast aeron channel for replicating from a leader to peers [aeron:ipc / aeron:udp?endpoint=224.0.1.1:40456]
    logsDirectory - directory to store raft log files

example commands:

./bin/raft-perf 0 3 100000 20000 SYNC aeron:ipc logs

./bin/raft-perf 1 3 100000 20000 SYNC aeron:ipc logs

./bin/raft-perf 2 3 100000 20000 SYNC aeron:ipc logs


