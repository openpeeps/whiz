## tests/bench_mem.nim — Memory benchmarks for whiz transports.
##
## Measures GC live/total memory around sustained loads plus the
## per-connection scratch pool bounds. Run:
##   nim c -d:release -r tests/bench_mem.nim
##
## Methodology: GC_fullCollect before each snapshot so `occupied` is live
## (retained) memory; `total` is allocator-held pages. Deltas across a
## fixed load show per-message retention (steady-state should be ~flat).

import whiz/pair
import whiz/ws
import whiz/curve
import std/[monotimes, strformat]

proc snap(): tuple[occupied, total: int64] =
  GC_fullCollect()
  (getOccupiedMem().int64, getTotalMem().int64)

proc report(label: string; n: int; size: int; before, after: tuple[occupied, total: int64]) =
  let occ = after.occupied - before.occupied
  let tot = after.total - before.total
  let perMsg = if n > 0: occ.float / n.float else: 0.0
  echo &"{label}: n={n} size={size} occupied {before.occupied} -> {after.occupied} (delta {occ}, {perMsg:.1f} B/msg) total {before.total} -> {after.total} (delta {tot})"

proc benchMemPairTcp() =
  const N = 20000
  const Size = 256
  const Port = 27510
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var finished = false
  let loop = newLoop()
  let srv = newPairSocket(loop)
  srv.`bind`("127.0.0.1", Port)
  let cli = newPairSocket(loop)
  srv.onMessage = proc(data: openArray[byte]) =
    inc received
    if received >= N:
      finished = true
      loop.stop()
  let before = snap()
  discard loop.addTimer(50) do (id: int):
    cli.connect("127.0.0.1", Port)
  discard loop.addTimer(400) do (id: int):
    for i in 0 ..< N:
      discard cli.send(payload)
  discard loop.addTimer(30_000) do (id: int):
    if not finished: echo "TIMEOUT benchMemPairTcp (", received, "/", N, ")"
    loop.stop()
  loop.run()
  let after = snap()
  report("mem_pair_tcp", received, Size, before, after)
  cli.close(); srv.close(); loop.close()

proc benchMemSteady() =
  ## Same load twice back-to-back: round 2 delta must be ~flat,
  ## proving round 1 retention is reusable buffers, not a leak.
  const N = 20000
  const Size = 256
  const Port = 27514
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  let loop = newLoop()
  let srv = newPairSocket(loop)
  srv.`bind`("127.0.0.1", Port)
  let cli = newPairSocket(loop)
  var received = 0
  srv.onMessage = proc(data: openArray[byte]) = inc received
  discard loop.addTimer(50) do (id: int):
    cli.connect("127.0.0.1", Port)
  discard loop.addTimer(400) do (id: int):
    for i in 0 ..< N:
      discard cli.send(payload)
  discard loop.addTimer(30_000) do (id: int):
    loop.stop()
  let s0 = snap()
  loop.run()
  let s1 = snap()
  # round 2 on the same connection
  received = 0
  var done2 = false
  srv.onMessage = proc(data: openArray[byte]) =
    inc received
    if received >= N:
      done2 = true
      loop.stop()
  for i in 0 ..< N:
    discard cli.send(payload)
  discard loop.addTimer(30_000) do (id: int):
    if not done2: echo "TIMEOUT benchMemSteady round2 (", received, "/", N, ")"
    loop.stop()
  loop.run()
  let s2 = snap()
  report("mem_steady_r1", N, Size, s0, s1)
  report("mem_steady_r2", received, Size, s1, s2)
  cli.close(); srv.close(); loop.close()

proc benchMemWs() =
  const N = 10000
  const Size = 256
  const Port = 27511
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var finished = false
  let loop = newLoop()
  let srv = newWsPairSocket(loop)
  srv.`bind`("127.0.0.1", Port)
  let cli = newWsPairSocket(loop)
  srv.onMessage = proc(data: openArray[byte]) =
    inc received
    if received >= N:
      finished = true
      loop.stop()
  let before = snap()
  discard loop.addTimer(100) do (id: int):
    cli.connect("127.0.0.1", Port)
  discard loop.addTimer(500) do (id: int):
    for i in 0 ..< N:
      discard cli.send(payload)
  discard loop.addTimer(30_000) do (id: int):
    if not finished: echo "TIMEOUT benchMemWs (", received, "/", N, ")"
    loop.stop()
  loop.run()
  let after = snap()
  report("mem_ws_pair", received, Size, before, after)
  # Pool bounds after the burst (sender side owns the scratch).
  echo &"  ws client scratch: cap={cli.sendScratchCap()} len={cli.sendScratchLen()} (grows once, then flat)"
  cli.close(); srv.close(); loop.close()

proc benchMemCurve() =
  const N = 10000
  const Size = 256
  const Port = 27512
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  let (srvSec, srvPub) = generateCurveKeypair()
  let (cliSec, cliPub) = generateCurveKeypair()
  var received = 0
  var finished = false
  let loop = newLoop()
  let srv = newPairSocket(loop)
  srv.setCurveKeypair(srvPub, srvSec)
  srv.`bind`("127.0.0.1", Port)
  let cli = newPairSocket(loop)
  cli.setCurveClient(cliPub, cliSec, srvPub)
  srv.onMessage = proc(data: openArray[byte]) =
    inc received
    if received >= N:
      finished = true
      loop.stop()
  let before = snap()
  discard loop.addTimer(50) do (id: int):
    cli.connect("127.0.0.1", Port)
  discard loop.addTimer(600) do (id: int):
    for i in 0 ..< N:
      discard cli.send(payload)
  discard loop.addTimer(30_000) do (id: int):
    if not finished: echo "TIMEOUT benchMemCurve (", received, "/", N, ")"
    loop.stop()
  loop.run()
  let after = snap()
  report("mem_curve_pair", received, Size, before, after)
  cli.close(); srv.close(); loop.close()

proc benchScratchBound() =
  ## Proves the WS scratch pool stays bounded across growing frames:
  ## cap must equal the largest frame, not the sum of frames.
  const Port = 27513
  let loop = newLoop()
  let srv = newWsPairSocket(loop)
  srv.`bind`("127.0.0.1", Port)
  let cli = newWsPairSocket(loop)
  var received = 0
  const N = 60
  srv.onMessage = proc(data: openArray[byte]) =
    inc received
    if received >= N:
      loop.stop()
  discard loop.addTimer(100) do (id: int):
    cli.connect("127.0.0.1", Port)
  discard loop.addTimer(500) do (id: int):
    for i in 0 ..< N:
      let size = 100 + i * 1000 # 100B .. ~60KB, forces pool growth
      var s = newString(size)
      for j in 0 ..< size: s[j] = char(byte(j and 0xFF))
      discard cli.send(s)
  discard loop.addTimer(30_000) do (id: int):
    loop.stop()
  loop.run()
  # Largest frame: 100 + 59*1000 + 10 hdr = 59110. Pool grows
  # geometrically (doubling), so cap lands on 65536 — bounded, not cumulative.
  echo &"  scratch after growing frames: cap={cli.sendScratchCap()} len={cli.sendScratchLen()} (expect 59110 <= cap <= 131072)"
  doAssert received == N, "lost messages: " & $received & "/" & $N
  doAssert cli.sendScratchCap() >= 59110 and cli.sendScratchCap() <= 131072,
    "scratch cap unexpected: " & $cli.sendScratchCap()
  cli.close(); srv.close(); loop.close()

benchMemPairTcp()
benchMemWs()
benchMemCurve()
benchScratchBound()
benchMemSteady()
echo "bench_mem done"
