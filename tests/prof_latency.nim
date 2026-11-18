## tests/prof_latency.nim — Profiling harness: sustained ping-pong latency.
## Run ~50k round-trips so the sampler sees the steady-state path.
## Build: clue build tests/prof_latency.nim -d:release --debuginfo:on -o:/tmp/prof_lat
import whiz/pair
import std/[monotimes, strformat]

proc monoNs(): int64 {.inline.} = getMonoTime().ticks

const N = 50000
const Port = 27920
var replies = 0
var tSend = monoNs()
let loop = newLoop()
let srv = newPairSocket(loop)
srv.`bind`("127.0.0.1", Port)
let cli = newPairSocket(loop)
var t0 = monoNs()
srv.onMessage = proc(data: openArray[byte]) =
  discard srv.send("A")
cli.onMessage = proc(data: openArray[byte]) =
  inc replies
  if replies < N:
    tSend = monoNs()
    discard cli.send("A")
  else:
    let us = (monoNs() - t0) div 1000
    echo &"done n={N} total_us={us}"
    loop.stop()
discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", Port)
discard loop.addTimer(500) do (id: int):
  t0 = monoNs(); tSend = monoNs()
  discard cli.send("A")
discard loop.addTimer(60_000) do (id: int):
  echo "TIMEOUT replies=", replies
  loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
