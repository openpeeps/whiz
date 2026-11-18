## tests/bench_vs_zmq.nim — Head-to-head whiz vs ØMQ (C libzmq via pkg/zmq).
##
## Same scenarios, same payloads, same machine, back-to-back:
##   pair_latency    N=5000 x 1B      (round-trip)
##   pair_throughput N=10000 x 512B   (one-way flood, inter-arrival stats)
##   reqrep          N=5000 x 128B    (round-trip)
##   pushpull_1worker N=10000 x 128B  (one-way flood)
##
## Needs libzmq at runtime (MacPorts default shown):
##   DYLD_LIBRARY_PATH=/opt/local/lib nim c -d:release -r tests/bench_vs_zmq.nim
## Or build once and run with the env var set.

import whiz/pair
import whiz/reqrep
import whiz/pushpull
import zmq
import std/[monotimes, strformat, math, algorithm, strutils, os]

const WatchdogMs = 30_000

type BenchRow = object
  impl: string
  label: string
  n: int
  size: int
  totalNs: int64
  perMsgNs: seq[int64]

var allRows: seq[BenchRow]

proc monoNs(): int64 {.inline.} =
  getMonoTime().ticks

proc perMsgStats(ns: openArray[int64]): string =
  let n = ns.len
  if n == 0: return " - | - | - | - | - | - | -"
  let sorted = ns.sorted()
  proc p(pct: float): int64 =
    sorted[int(pct / 100.0 * (n - 1).float)]
  let meanNs = sorted.sum div n
  var variance = 0.0
  for v in sorted:
    variance += ((v - meanNs).float^2)
  let stddevNs = sqrt(variance).int64
  &"{sorted[0].float/1000:.1f} | {p(50).float/1000:.1f} | {p(75).float/1000:.1f} | {p(90).float/1000:.1f} | {p(99).float/1000:.1f} | {meanNs.float/1000:.1f} | {stddevNs.float/1000:.1f}"

proc renderTable(): string =
  var lines: seq[string]
  lines.add "| impl | benchmark | n | size | total(μs) | throughput/s | min(μs) | p50(μs) | p75(μs) | p90(μs) | p99(μs) | avg(μs) | σ(μs) |"
  lines.add "|---|---|---|---|---|---|---|---|---|---|---|---|---|"
  for r in allRows:
    let tUs = r.totalNs div 1000
    let throughput = if r.totalNs > 0: int64(r.n.float / (r.totalNs.float / 1_000_000_000.0)) else: 0
    lines.add &"| {r.impl} | {r.label} | {r.n} | {r.size} | {tUs} | {throughput} | {perMsgStats(r.perMsgNs)} |"
  lines.join("\n")

# ── whiz side (same methodology as bench_zmtp.nim) ────────────────────────────

proc whizPairLatency(port: int) =
  const N = 5000
  const Payload = "A"
  var replies = 0
  var finished = false
  var perMsg: seq[int64]
  var tSend = monoNs()
  let loop = newLoop()
  let srv = newPairSocket(loop)
  srv.`bind`("127.0.0.1", port)
  let cli = newPairSocket(loop)
  srv.onMessage = proc(data: openArray[byte]) = discard srv.send(Payload)
  cli.onMessage = proc(data: openArray[byte]) =
    perMsg.add(monoNs() - tSend)
    inc replies
    if replies < N:
      tSend = monoNs()
      discard cli.send(Payload)
    else:
      finished = true
      loop.stop()
  var t0 = monoNs()
  discard loop.addTimer(50) do (id: int):
    cli.connect("127.0.0.1", port)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs(); tSend = monoNs()
    discard cli.send(Payload)
  discard loop.addTimer(WatchdogMs) do (id: int):
    if not finished: echo "TIMEOUT whiz pair_latency"
    loop.stop()
  loop.run()
  let elapsed = monoNs() - t0
  if finished:
    allRows.add BenchRow(impl: "whiz", label: "pair_latency", n: N, size: 1, totalNs: elapsed, perMsgNs: perMsg)
  cli.close(); srv.close(); loop.close()

proc whizPairThroughput(port: int) =
  const N = 10000
  const Size = 512
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var finished = false
  var elapsed = 0i64
  var t0 = 0i64
  var recvTs: seq[int64]
  let loop = newLoop()
  let srv = newPairSocket(loop)
  srv.`bind`("127.0.0.1", port)
  let cli = newPairSocket(loop)
  cli.setBatching(true) # flood: coalesce into bulk writes
  srv.onMessage = proc(data: openArray[byte]) =
    recvTs.add(monoNs())
    inc received
    if received >= N:
      elapsed = monoNs() - t0
      finished = true
      loop.stop()
  discard loop.addTimer(50) do (id: int):
    cli.connect("127.0.0.1", port)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      discard cli.send(payload)
  discard loop.addTimer(WatchdogMs) do (id: int):
    if not finished: echo "TIMEOUT whiz pair_throughput (", received, ")"
    loop.stop()
  loop.run()
  if finished:
    var deltas: seq[int64]
    for i in 1 ..< recvTs.len: deltas.add(recvTs[i] - recvTs[i-1])
    allRows.add BenchRow(impl: "whiz", label: "pair_throughput", n: N, size: Size, totalNs: elapsed, perMsgNs: deltas)
  cli.close(); srv.close(); loop.close()

proc whizReqrep(port: int) =
  const N = 5000
  const Size = 128
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  var replies = 0
  var finished = false
  var perMsg: seq[int64]
  var tSend = monoNs()
  let loop = newLoop()
  let rep = newRepSocket(loop)
  rep.`bind`("127.0.0.1", port)
  let req = newReqSocket(loop)
  rep.onRequest = proc(data: openArray[byte]) = discard rep.send("r")
  req.onReply = proc(data: openArray[byte]) =
    perMsg.add(monoNs() - tSend)
    inc replies
    if replies < N:
      tSend = monoNs()
      discard req.send(payload)
    else:
      finished = true
      loop.stop()
  var t0 = monoNs()
  discard loop.addTimer(50) do (id: int):
    req.connect("127.0.0.1", port)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs(); tSend = monoNs()
    discard req.send(payload)
  discard loop.addTimer(WatchdogMs) do (id: int):
    if not finished: echo "TIMEOUT whiz reqrep"
    loop.stop()
  loop.run()
  let elapsed = monoNs() - t0
  if finished:
    allRows.add BenchRow(impl: "whiz", label: "reqrep", n: N, size: Size, totalNs: elapsed, perMsgNs: perMsg)
  req.close(); rep.close(); loop.close()

proc whizPushpull(port: int) =
  const N = 10000
  const Size = 128
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var finished = false
  var elapsed = 0i64
  var t0 = 0i64
  var recvTs: seq[int64]
  let loop = newLoop()
  let push = newPushSocket(loop)
  push.setBatching(true) # flood: coalesce into bulk writes
  push.`bind`("127.0.0.1", port)
  let pull = newPullSocket(loop)
  pull.onMessage = proc(data: openArray[byte]) =
    recvTs.add(monoNs())
    inc received
    if received >= N:
      elapsed = monoNs() - t0
      finished = true
      loop.stop()
  discard loop.addTimer(50) do (id: int):
    pull.connect("127.0.0.1", port)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      discard push.send(payload)
  discard loop.addTimer(WatchdogMs) do (id: int):
    if not finished: echo "TIMEOUT whiz pushpull (", received, ")"
    loop.stop()
  loop.run()
  if finished:
    var deltas: seq[int64]
    for i in 1 ..< recvTs.len: deltas.add(recvTs[i] - recvTs[i-1])
    allRows.add BenchRow(impl: "whiz", label: "pushpull_1worker", n: N, size: Size, totalNs: elapsed, perMsgNs: deltas)
  push.close(); pull.close(); loop.close()

# ── ØMQ side (blocking API; peer runs on a helper thread) ────────────────────
#
# Threads can't touch GC'd globals, so each server thread ships its
# results back through a channel (join synchronizes handoff).

type FloodResult = tuple[stamps: seq[int64], got: int, doneTs: int64]

var zN = 0
var zChan: Channel[FloodResult]

proc zPairEchoServer(port: int) {.thread.} =
  let s = zmq.listen("tcp://127.0.0.1:" & $port, PAIR)
  for i in 0 ..< zN:
    let m = zmq.waitForReceive(s, 30_000)
    if not m.msgAvailable: break
    zmq.send(s, m.msg)
  zmq.close(s)

proc zmqPairLatency(port: int) =
  const N = 5000
  zN = N
  var thr: Thread[int]
  createThread(thr, zPairEchoServer, port)
  sleep(300)
  let c = zmq.connect("tcp://127.0.0.1:" & $port, PAIR)
  var perMsg: seq[int64]
  let t0 = monoNs()
  for i in 0 ..< N:
    let tSend = monoNs()
    zmq.send(c, "A")
    let r = zmq.waitForReceive(c, 30_000)
    perMsg.add(monoNs() - tSend)
    if not r.msgAvailable:
      echo "TIMEOUT zmq pair_latency at ", i
      break
  let elapsed = monoNs() - t0
  joinThread(thr)
  zmq.close(c)
  if perMsg.len == N:
    allRows.add BenchRow(impl: "zmq", label: "pair_latency", n: N, size: 1, totalNs: elapsed, perMsgNs: perMsg)

proc zFloodServerPair(port: int) {.thread.} =
  var stamps: seq[int64] = @[]
  var got = 0
  let s = zmq.listen("tcp://127.0.0.1:" & $port, PAIR)
  for i in 0 ..< zN:
    let m = zmq.waitForReceive(s, 30_000)
    if not m.msgAvailable: break
    stamps.add(monoNs())
    inc got
  let doneTs = monoNs()
  zmq.close(s)
  zChan.send((stamps, got, doneTs))

proc zmqPairThroughput(port: int) =
  const N = 10000
  const Size = 512
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  zN = N
  zChan.open()
  var thr: Thread[int]
  createThread(thr, zFloodServerPair, port)
  sleep(300)
  let c = zmq.connect("tcp://127.0.0.1:" & $port, PAIR)
  sleep(100)
  let t0 = monoNs()
  for i in 0 ..< N:
    zmq.send(c, payload)
  joinThread(thr)
  let res = zChan.recv()
  zChan.close()
  let elapsed = res.doneTs - t0
  zmq.close(c)
  if res.got == N:
    var deltas: seq[int64]
    for i in 1 ..< res.stamps.len: deltas.add(res.stamps[i] - res.stamps[i-1])
    allRows.add BenchRow(impl: "zmq", label: "pair_throughput", n: N, size: Size, totalNs: elapsed, perMsgNs: deltas)
  else:
    echo "TIMEOUT zmq pair_throughput (", res.got, "/", N, ")"

proc zRepServer(port: int) {.thread.} =
  let s = zmq.listen("tcp://127.0.0.1:" & $port, REP)
  for i in 0 ..< zN:
    let m = zmq.waitForReceive(s, 30_000)
    if not m.msgAvailable: break
    zmq.send(s, "r")
  zmq.close(s)

proc zmqReqrep(port: int) =
  const N = 5000
  const Size = 128
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  zN = N
  var thr: Thread[int]
  createThread(thr, zRepServer, port)
  sleep(300)
  let c = zmq.connect("tcp://127.0.0.1:" & $port, REQ)
  var perMsg: seq[int64]
  let t0 = monoNs()
  for i in 0 ..< N:
    let tSend = monoNs()
    zmq.send(c, payload)
    let r = zmq.waitForReceive(c, 30_000)
    perMsg.add(monoNs() - tSend)
    if not r.msgAvailable:
      echo "TIMEOUT zmq reqrep at ", i
      break
  let elapsed = monoNs() - t0
  joinThread(thr)
  zmq.close(c)
  if perMsg.len == N:
    allRows.add BenchRow(impl: "zmq", label: "reqrep", n: N, size: Size, totalNs: elapsed, perMsgNs: perMsg)

proc zPullServer(port: int) {.thread.} =
  var stamps: seq[int64] = @[]
  var got = 0
  let s = zmq.listen("tcp://127.0.0.1:" & $port, PULL)
  for i in 0 ..< zN:
    let m = zmq.waitForReceive(s, 30_000)
    if not m.msgAvailable: break
    stamps.add(monoNs())
    inc got
  let doneTs = monoNs()
  zmq.close(s)
  zChan.send((stamps, got, doneTs))

proc zmqPushpull(port: int) =
  const N = 10000
  const Size = 128
  var payload = newString(Size)
  for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
  zN = N
  zChan.open()
  var thr: Thread[int]
  createThread(thr, zPullServer, port)
  sleep(300)
  let c = zmq.connect("tcp://127.0.0.1:" & $port, PUSH)
  sleep(100)
  let t0 = monoNs()
  for i in 0 ..< N:
    zmq.send(c, payload)
  joinThread(thr)
  let res = zChan.recv()
  zChan.close()
  let elapsed = res.doneTs - t0
  zmq.close(c)
  if res.got == N:
    var deltas: seq[int64]
    for i in 1 ..< res.stamps.len: deltas.add(res.stamps[i] - res.stamps[i-1])
    allRows.add BenchRow(impl: "zmq", label: "pushpull_1worker", n: N, size: Size, totalNs: elapsed, perMsgNs: deltas)
  else:
    echo "TIMEOUT zmq pushpull (", res.got, "/", N, ")"

whizPairLatency(27600)
whizPairThroughput(27601)
whizReqrep(27602)
whizPushpull(27603)
zmqPairLatency(27700)
zmqPairThroughput(27701)
zmqReqrep(27702)
zmqPushpull(27703)
echo renderTable()
