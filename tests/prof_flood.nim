## tests/prof_flood.nim — Profiling harness: sustained paced flood.
## Paces sends in per-tick batches so send and drain interleave (steady
## state, not a single burst that trips the 32MB write cap). Keeps the
## process busy for ~1-2s so the Time Profiler gets enough samples.
## Build: clue build tests/prof_flood.nim -d:release --debuginfo:on -o:/tmp/prof_flood
## Profile: xcrun xctrace record --template 'Time Profiler' --output /tmp/prof.trace --launch -- /tmp/prof_flood
import whiz/pair
import std/[monotimes, strformat]

proc monoNs(): int64 {.inline.} =
  getMonoTime().ticks

const N = 300000
const Batch = 6000
const Size = 512
const Port = 27900
const TickMs = 10
const Rounds = 8

var payload = newString(Size)
for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
var received = 0
var sent = 0
var t0 = monoNs()
let loop = newLoop()
let srv = newPairSocket(loop)
srv.`bind`("127.0.0.1", Port)
let cli = newPairSocket(loop)
cli.setBatching(true)
srv.onMessage = proc(data: openArray[byte]) =
  inc received
  if received >= N * Rounds:
    let us = (monoNs() - t0) div 1000
    echo &"done n={received} size={Size} total_us={us} throughput={received.float / (us.float / 1_000_000.0):.0f}/s"
    loop.stop()
discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", Port)
discard loop.addTimer(400) do (id: int):
  t0 = monoNs()
  proc tick() =
    for i in 0 ..< Batch:
      if sent >= N * Rounds: break
      discard cli.send(payload)
      inc sent
    if sent < N * Rounds:
      discard loop.addTimer(TickMs) do (id2: int):
        tick()
  tick()
discard loop.addTimer(60_000) do (id: int):
  echo "TIMEOUT received=", received, " sent=", sent
  loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
