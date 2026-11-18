## tests/prof_threaded.nim — EXPERIMENT: sender and receiver on separate
## loops/threads (overlap I/O like libzmq's I/O thread). Measures whether
## single-loop alternation is the remaining burst bottleneck.
import whiz/pair
import std/[monotimes, strformat, os]

proc monoNs(): int64 {.inline.} = getMonoTime().ticks

const N = 20000
const Size = 512
const Port = 27910

var payload {.global.}: string
var received {.global.}: int
var t0 {.global.}: int64

payload = newString(Size)
for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char

proc runServer(port: int) {.thread.} =
  {.cast(gcsafe).}:
    let loop = newLoop()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    srv.onMessage = proc(data: openArray[byte]) =
      inc received
      if received >= N:
        let us = (monoNs() - t0) div 1000
        echo "threaded done n=", received, " total_us=", us,
          " throughput=", (received.float / (us.float / 1_000_000.0)).int, "/s"
        loop.stop()
    loop.run()
    srv.close()
    loop.close()

var srvThread: Thread[int]
createThread(srvThread, runServer, Port)
sleep(300)

let cliLoop = newLoop()
let cli = newPairSocket(cliLoop)
cli.setBatching(true)
discard cliLoop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", Port)
discard cliLoop.addTimer(400) do (id: int):
  t0 = monoNs()
  for i in 0 ..< N:
    discard cli.send(payload)
  # poll for completion (the server thread counts arrivals)
  proc pollDone() =
    if received >= N:
      cliLoop.stop()
    else:
      discard cliLoop.addTimer(100) do (id2: int):
        pollDone()
  pollDone()
discard cliLoop.addTimer(60_000) do (id: int):
  echo "TIMEOUT received=", received
  cliLoop.stop()
cliLoop.run()
cli.close()
cliLoop.close()
joinThread(srvThread)
