## tests/bench_zmtp.nim — Benchmarks for ZMTP 3.0 socket patterns.
##
## Measures latency and throughput for PAIR, PUB/SUB, and REQ/REP
## over the ZMTP wire protocol on localhost TCP.
##
## Run:  nim c -d:release -r tests/bench_zmtp.nim

import whiz/pair
import whiz/pubsub
import whiz/reqrep
import whiz/pushpull
import std/[monotimes, strformat, math, algorithm, strutils, os]

var nextPort = 26000
proc allocPort: int = result = nextPort; inc nextPort

proc monoNs(): int64 {.inline.} =
  getMonoTime().ticks

type BenchRow = object
  label: string
  subscriber: string
  n: int
  size: int
  totalNs: int64
  perMsgNs: seq[int64]

var allRows: seq[BenchRow]

type BenchCfg = object
  transport: Transport
  address: string
  port: int

proc tcp(label: string): BenchCfg =
  BenchCfg(transport: TransportTcp, address: "127.0.0.1", port: allocPort())

proc unix(label: string): BenchCfg =
  let p = "/tmp/whiz_bench_" & label
  removeFile(p)
  BenchCfg(transport: TransportUnix, address: p, port: 0)

proc cleanup(cfg: BenchCfg) =
  if cfg.transport == TransportUnix:
    removeFile(cfg.address)

proc transportSuffix(transport: Transport): string =
  if transport == TransportTcp: "_tcp" else: "_unix"

proc fmtNs(ns: int64): string =
  &"{ns.float / 1000.0:.1f}"

proc perMsgStats(ns: openArray[int64]): string =
  let n = ns.len
  if n == 0: return " - | - | - | - | - | - | -"
  let sorted = ns.sorted()
  proc p(pct: float): int64 =
    sorted[int(pct / 100.0 * (n - 1).float)]
  let minNs = sorted[0]
  let p50ns = p(50)
  let p75ns = p(75)
  let p90ns = p(90)
  let p99ns = p(99)
  let meanNs = sorted.sum div n
  var variance = 0.0
  for v in sorted:
    variance += ((v - meanNs).float^2)
  variance /= n.float
  let stddevNs = sqrt(variance).int64
  &"{fmtNs(minNs)} | {fmtNs(p50ns)} | {fmtNs(p75ns)} | {fmtNs(p90ns)} | {fmtNs(p99ns)} | {fmtNs(meanNs)} | {fmtNs(stddevNs)}"

proc renderTable: string =
  var lines: seq[string]
  lines.add "| benchmark | subscriber | n | size | total(μs) | throughput/s | min(μs) | p50(μs) | p75(μs) | p90(μs) | p99(μs) | avg(μs) | σ(μs) |"
  lines.add "|---|---|---|---|---|---|---|---|---|---|---|---|---|"
  for r in allRows:
    let tUs = r.totalNs div 1000
    let throughput = if r.totalNs > 0: int64(r.n.float / (r.totalNs.float / 1_000_000_000.0)) else: 0
    let sub = if r.subscriber.len > 0: r.subscriber else: "-"
    lines.add &"| {r.label} | {sub} | {r.n} | {r.size} | {tUs} | {throughput} | {perMsgStats(r.perMsgNs)} |"
  lines.join("\n")

proc bench_pair_latency(cfg: BenchCfg) =
  const N = 5000
  const Payload = "A"
  var replies = 0
  var perMsg: seq[int64]
  var tSend: int64
  let loop = newLoop()
  let srv = newPairSocket(loop)
  srv.`bind`(cfg.address, cfg.port, cfg.transport)
  let cli = newPairSocket(loop)
  srv.onMessage = proc(data: openArray[byte]) {.closure.} =
    srv.send(Payload)
  cli.onMessage = proc(data: openArray[byte]) {.closure.} =
    perMsg.add(monoNs() - tSend)
    inc replies
    if replies < N:
      tSend = monoNs()
      cli.send(Payload)
    else:
      loop.stop()
  var t0 = monoNs()
  discard loop.addTimer(50) do (id: int):
    cli.connect(cfg.address, cfg.port, cfg.transport)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    tSend = monoNs()
    cli.send(Payload)
  loop.run()
  let elapsed = monoNs() - t0
  allRows.add BenchRow(label: "bench_pair_latency" & transportSuffix(cfg.transport), subscriber: "-", n: N, size: Payload.len, totalNs: elapsed, perMsgNs: perMsg)
  cli.close()
  srv.close()
  loop.close()

proc bench_pair_throughput(cfg: BenchCfg) =
  const N = 10000
  const PayloadLen = 512
  var payload = newString(PayloadLen)
  for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var elapsed = 0i64
  var t0 = 0i64
  var recvTs: seq[int64]
  let loop = newLoop()
  let srv = newPairSocket(loop)
  srv.`bind`(cfg.address, cfg.port, cfg.transport)
  let cli = newPairSocket(loop)
  srv.onMessage = proc(data: openArray[byte]) {.closure.} =
    recvTs.add(monoNs())
    inc received
    if received >= N:
      elapsed = monoNs() - t0
      loop.stop()
  discard loop.addTimer(50) do (id: int):
    cli.connect(cfg.address, cfg.port, cfg.transport)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      cli.send(payload)
  loop.run()
  var deltas: seq[int64]
  for i in 1 ..< recvTs.len:
    deltas.add(recvTs[i] - recvTs[i-1])
  allRows.add BenchRow(label: "bench_pair_throughput" & transportSuffix(cfg.transport), subscriber: "-", n: N, size: PayloadLen, totalNs: elapsed, perMsgNs: deltas)
  cli.close()
  srv.close()
  loop.close()

proc bench_pubsub_1sub(cfg: BenchCfg) =
  ## Subscribe before connect so SUBSCRIBE is sent immediately on handshake completion
  const N = 10000
  const PayloadLen = 512
  var payload = newString(PayloadLen)
  for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var recvTs: seq[int64]
  let loop = newLoop()
  let pub = newPubSocket(loop, cfg.address, cfg.port, cfg.transport)
  let sub = newSubSocket(loop)
  sub.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
    recvTs.add(monoNs())
    inc received
    if received >= N: loop.stop()
  sub.subscribe("")
  discard loop.addTimer(50) do (id: int):
    sub.connect(cfg.address, cfg.port, cfg.transport)
  var t0 = monoNs()
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      pub.publish(payload)
  loop.run()
  let elapsed = monoNs() - t0
  var deltas: seq[int64]
  for i in 1 ..< recvTs.len:
    deltas.add(recvTs[i] - recvTs[i-1])
  allRows.add BenchRow(label: "bench_pubsub_1sub" & transportSuffix(cfg.transport), subscriber: "-", n: N, size: PayloadLen, totalNs: elapsed, perMsgNs: deltas)
  sub.close()
  pub.close()
  loop.close()

proc bench_pubsub_2sub(cfg: BenchCfg) =
  ## Subscribe before connect so SUBSCRIBE is sent immediately on handshake completion
  const N = 5000
  const PayloadLen = 256
  var payload = newString(PayloadLen)
  for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
  var a, b = 0
  var recvTsA, recvTsB: seq[int64]
  let loop = newLoop()
  let pub = newPubSocket(loop, cfg.address, cfg.port, cfg.transport)
  let sub1 = newSubSocket(loop)
  let sub2 = newSubSocket(loop)
  sub1.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
    recvTsA.add(monoNs())
    inc a; if a >= N: loop.stop()
  sub2.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
    recvTsB.add(monoNs())
    inc b; if b >= N: loop.stop()
  sub1.subscribe("")
  sub2.subscribe("")
  discard loop.addTimer(50) do (id: int):
    sub1.connect(cfg.address, cfg.port, cfg.transport)
    sub2.connect(cfg.address, cfg.port, cfg.transport)
  var t0 = monoNs()
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      pub.publish(payload)
  loop.run()
  let elapsed = monoNs() - t0
  proc deltas(ts: seq[int64]): seq[int64] =
    result = newSeq[int64](max(0, ts.len - 1))
    for i in 1 ..< ts.len:
      result[i-1] = ts[i] - ts[i-1]
  let d0 = deltas(recvTsA)
  let d1 = deltas(recvTsB)
  let sfx = transportSuffix(cfg.transport)
  allRows.add BenchRow(label: "bench_pubsub_2sub" & sfx, subscriber: "sub0", n: a, size: PayloadLen, totalNs: elapsed, perMsgNs: d0)
  allRows.add BenchRow(label: "bench_pubsub_2sub" & sfx, subscriber: "sub1", n: b, size: PayloadLen, totalNs: elapsed, perMsgNs: d1)
  allRows.add BenchRow(label: "bench_pubsub_2sub" & sfx, subscriber: "total", n: a+b, size: PayloadLen, totalNs: elapsed, perMsgNs: d0 & d1)
  sub1.close()
  sub2.close()
  pub.close()
  loop.close()

proc bench_reqrep(cfg: BenchCfg) =
  const N = 5000
  const PayloadLen = 128
  var payload = newString(PayloadLen)
  for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
  var replies = 0
  var perMsg: seq[int64]
  var tSend: int64
  let loop = newLoop()
  let rep = newRepSocket(loop)
  rep.`bind`(cfg.address, cfg.port, cfg.transport)
  let req = newReqSocket(loop)
  rep.onRequest = proc(data: openArray[byte]) {.closure.} =
    rep.send(payload)
  req.onReply = proc(data: openArray[byte]) {.closure.} =
    perMsg.add(monoNs() - tSend)
    inc replies
    if replies < N:
      tSend = monoNs()
      req.send(payload)
    else:
      loop.stop()
  var t0 = monoNs()
  discard loop.addTimer(50) do (id: int):
    req.connect(cfg.address, cfg.port, cfg.transport)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    tSend = monoNs()
    req.send(payload)
  loop.run()
  let elapsed = monoNs() - t0
  allRows.add BenchRow(label: "bench_reqrep" & transportSuffix(cfg.transport), subscriber: "-", n: N, size: PayloadLen, totalNs: elapsed, perMsgNs: perMsg)
  req.close()
  rep.close()
  loop.close()

proc bench_sizes_pair(cfg: BenchCfg) =
  let sfx = transportSuffix(cfg.transport)
  for size in [64, 1024, 65536]:
    const N = 2000
    var payload = newString(size)
    for i in 0 ..< size: payload[i] = byte((i and 0x7F) + 32).char
    var received = 0
    var t0 = 0i64
    var recvTs: seq[int64]
    let loop = newLoop()
    let srv = newPairSocket(loop)
    srv.`bind`(cfg.address, cfg.port, cfg.transport)
    let cli = newPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      recvTs.add(monoNs())
      inc received
      if received >= N: loop.stop()
    discard loop.addTimer(50) do (id: int):
      cli.connect(cfg.address, cfg.port, cfg.transport)
    discard loop.addTimer(500) do (id: int):
      t0 = monoNs()
      for i in 0 ..< N:
        cli.send(payload)
    loop.run()
    let elapsed = monoNs() - t0
    var deltas: seq[int64]
    for i in 1 ..< recvTs.len:
      deltas.add(recvTs[i] - recvTs[i-1])
    allRows.add BenchRow(label: "bench_sizes_pair" & sfx, subscriber: "-", n: N, size: size, totalNs: elapsed, perMsgNs: deltas)
    cli.close()
    srv.close()
    loop.close()

proc bench_pushpull_throughput(cfg: BenchCfg) =
  const N = 10000
  const PayloadLen = 512
  var payload = newString(PayloadLen)
  for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var recvTs: seq[int64]
  let loop = newLoop()
  let push = newPushSocket(loop)
  push.`bind`(cfg.address, cfg.port, cfg.transport)
  let pull = newPullSocket(loop)
  pull.onMessage = proc(data: openArray[byte]) {.closure.} =
    recvTs.add(monoNs())
    inc received
    if received >= N: loop.stop()
  var t0 = monoNs()
  discard loop.addTimer(50) do (id: int):
    pull.connect(cfg.address, cfg.port, cfg.transport)
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      push.send(payload)
  loop.run()
  let elapsed = monoNs() - t0
  var deltas: seq[int64]
  for i in 1 ..< recvTs.len:
    deltas.add(recvTs[i] - recvTs[i-1])
  allRows.add BenchRow(label: "bench_pushpull_throughput" & transportSuffix(cfg.transport), subscriber: "-", n: N, size: PayloadLen, totalNs: elapsed, perMsgNs: deltas)
  pull.close()
  push.close()
  loop.close()

proc bench_pushpull_1worker(cfg: BenchCfg) =
  const N = 10000
  const PayloadLen = 128
  var payload = newString(PayloadLen)
  for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
  var received = 0
  var recvTs: seq[int64]
  let loop = newLoop()
  let push = newPushSocket(loop)
  push.`bind`(cfg.address, cfg.port, cfg.transport)
  let pull = newPullSocket(loop)
  pull.onMessage = proc(data: openArray[byte]) {.closure.} =
    recvTs.add(monoNs())
    inc received
    if received >= N: loop.stop()
  discard loop.addTimer(50) do (id: int):
    pull.connect(cfg.address, cfg.port, cfg.transport)
  var t0 = monoNs()
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      push.send(payload)
  loop.run()
  let elapsed = monoNs() - t0
  var deltas: seq[int64]
  for i in 1 ..< recvTs.len:
    deltas.add(recvTs[i] - recvTs[i-1])
  allRows.add BenchRow(label: "bench_pushpull_1worker" & transportSuffix(cfg.transport), subscriber: "-", n: N, size: PayloadLen, totalNs: elapsed, perMsgNs: deltas)
  pull.close()
  push.close()
  loop.close()

proc bench_pushpull_3workers(cfg: BenchCfg) =
  const N = 5000
  const PayloadLen = 128
  var payload = newString(PayloadLen)
  for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
  var received: array[3, int]
  var recvTs: array[3, seq[int64]]
  for i in 0 ..< 3: recvTs[i] = @[]
  let loop = newLoop()
  let push = newPushSocket(loop)
  push.`bind`(cfg.address, cfg.port, cfg.transport)
  var pulls: array[3, PullSocket]
  proc onMsg(i: int): proc(data: openArray[byte]) {.closure.} =
    result = proc(data: openArray[byte]) {.closure.} =
      recvTs[i].add(monoNs())
      inc received[i]
      var total = 0
      for j in 0 ..< 3: total += received[j]
      if total >= N: loop.stop()
  for i in 0 ..< 3:
    pulls[i] = newPullSocket(loop)
    pulls[i].onMessage = onMsg(i)
  discard loop.addTimer(50) do (id: int):
    for i in 0 ..< 3:
      pulls[i].connect(cfg.address, cfg.port, cfg.transport)
  var t0 = monoNs()
  discard loop.addTimer(500) do (id: int):
    t0 = monoNs()
    for i in 0 ..< N:
      push.send(payload)
  loop.run()
  let elapsed = monoNs() - t0
  let sfx = transportSuffix(cfg.transport)
  var allDeltas: seq[int64]
  for i in 0 ..< 3:
    var wd: seq[int64]
    for j in 1 ..< recvTs[i].len:
      wd.add(recvTs[i][j] - recvTs[i][j-1])
    allDeltas.add(wd)
    allRows.add BenchRow(label: "bench_pushpull_3workers" & sfx, subscriber: &"worker{i}", n: received[i], size: PayloadLen, totalNs: elapsed, perMsgNs: wd)
  allRows.add BenchRow(label: "bench_pushpull_3workers" & sfx, subscriber: "total", n: received[0]+received[1]+received[2], size: PayloadLen, totalNs: elapsed, perMsgNs: allDeltas)
  for i in 0 ..< 3: pulls[i].close()
  push.close()
  loop.close()

for cfg in [tcp("pair_latency"), unix("pair_latency")]:
  bench_pair_latency(cfg); cleanup(cfg)
for cfg in [tcp("pair_throughput"), unix("pair_throughput")]:
  bench_pair_throughput(cfg); cleanup(cfg)
for cfg in [tcp("pubsub_1sub"), unix("pubsub_1sub")]:
  bench_pubsub_1sub(cfg); cleanup(cfg)
for cfg in [tcp("pubsub_2sub"), unix("pubsub_2sub")]:
  bench_pubsub_2sub(cfg); cleanup(cfg)
for cfg in [tcp("reqrep"), unix("reqrep")]:
  bench_reqrep(cfg); cleanup(cfg)
for cfg in [tcp("sizes_pair"), unix("sizes_pair")]:
  bench_sizes_pair(cfg); cleanup(cfg)
for cfg in [tcp("pushpull_throughput"), unix("pushpull_throughput")]:
  bench_pushpull_throughput(cfg); cleanup(cfg)
for cfg in [tcp("pushpull_1worker"), unix("pushpull_1worker")]:
  bench_pushpull_1worker(cfg); cleanup(cfg)
for cfg in [tcp("pushpull_3workers"), unix("pushpull_3workers")]:
  bench_pushpull_3workers(cfg); cleanup(cfg)

echo renderTable()
