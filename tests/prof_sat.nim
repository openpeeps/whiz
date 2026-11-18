## tests/prof_sat.nim — Sustained saturation: back-to-back bursts, no idle.
## Each round floods N msgs synchronously; the next round starts the
## moment the last message arrives (chained in onMessage). Keeps the
## loop saturated for profiling the true steady-state hot path.
## Build: clue build tests/prof_sat.nim -d:release --debuginfo:on -o:/tmp/prof_sat
import whiz/pair
import std/[monotimes, strformat]

proc monoNs(): int64 {.inline.} = getMonoTime().ticks

const N = 20000
const Rounds = 15
const Size = 512
const Port = 27911

var payload = newString(Size)
for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
var received = 0
var round = 0
var t0 = monoNs()
let loop = newLoop()
let srv = newPairSocket(loop)
srv.`bind`("127.0.0.1", Port)
let cli = newPairSocket(loop)
cli.setBatching(true)

proc flood() =
  for i in 0 ..< N:
    discard cli.send(payload)

srv.onMessage = proc(data: openArray[byte]) =
  inc received
  if received >= N * Rounds:
    let us = (monoNs() - t0) div 1000
    echo &"done n={received} total_us={us} throughput={received.float / (us.float / 1_000_000.0):.0f}/s"
    loop.stop()
  elif received mod N == 0:
    inc round
    flood()

discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", Port)
discard loop.addTimer(500) do (id: int):
  t0 = monoNs()
  flood()
discard loop.addTimer(120_000) do (id: int):
  echo "TIMEOUT received=", received
  loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
