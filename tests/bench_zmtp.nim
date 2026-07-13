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
import std/[monotimes, strformat, unittest]

var nextPort = 26000
proc allocPort: int = result = nextPort; inc nextPort

proc monoUs(): int64 {.inline.} =
  getMonoTime().ticks div 1_000

suite "bench_zmtp":

  test "bench_pair_latency":
    const N = 5000
    const Payload = "A"
    var replies = 0
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      srv.send(Payload)
    cli.onMessage = proc(data: openArray[byte]) {.closure.} =
      inc replies
      if replies < N:
        cli.send(Payload)
      else:
        loop.stop()
    var t0 = monoUs()
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      cli.send(Payload)
    loop.run()
    let elapsed = monoUs() - t0
    echo &"  bench_pair_latency  N={N}  size={Payload.len}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
    cli.close()
    srv.close()
    loop.close()

  test "bench_pair_throughput":
    const N = 10000
    const PayloadLen = 512
    var payload = newString(PayloadLen)
    for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
    var received = 0
    var elapsed = 0i64
    var t0 = 0i64
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      inc received
      if received >= N:
        elapsed = monoUs() - t0
        loop.stop()
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      for i in 0 ..< N:
        cli.send(payload)
    loop.run()
    echo &"  bench_pair_throughput  N={N}  size={PayloadLen}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
    cli.close()
    srv.close()
    loop.close()

  test "bench_pubsub_1sub":
    ## Subscribe before connect so SUBSCRIBE is sent immediately on handshake completion
    const N = 10000
    const PayloadLen = 512
    var payload = newString(PayloadLen)
    for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let pub = newPubSocket(loop, "127.0.0.1", port)
    let sub = newSubSocket(loop)
    sub.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
      inc received
      if received >= N: loop.stop()
    sub.subscribe("")
    discard loop.addTimer(50) do (id: int):
      sub.connect("127.0.0.1", port)
    var t0 = monoUs()
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      for i in 0 ..< N:
        pub.publish(payload)
    loop.run()
    let elapsed = monoUs() - t0
    echo &"  bench_pubsub_1sub  N={N}  size={PayloadLen}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
    sub.close()
    pub.close()
    loop.close()

  test "bench_pubsub_2sub":
    ## Subscribe before connect so SUBSCRIBE is sent immediately on handshake completion
    const N = 5000
    const PayloadLen = 256
    var payload = newString(PayloadLen)
    for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
    var a, b = 0
    let loop = newLoop()
    let port = allocPort()
    let pub = newPubSocket(loop, "127.0.0.1", port)
    let sub1 = newSubSocket(loop)
    let sub2 = newSubSocket(loop)
    sub1.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
      inc a; if a >= N: loop.stop()
    sub2.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
      inc b; if b >= N: loop.stop()
    sub1.subscribe("")
    sub2.subscribe("")
    discard loop.addTimer(50) do (id: int):
      sub1.connect("127.0.0.1", port)
      sub2.connect("127.0.0.1", port)
    var t0 = monoUs()
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      for i in 0 ..< N:
        pub.publish(payload)
    loop.run()
    let elapsed = monoUs() - t0
    echo &"  bench_pubsub_2sub  N={N}  size={PayloadLen}  {elapsed}us  {(N * 2 * 1_000_000 div max(elapsed, 1))}/s"
    sub1.close()
    sub2.close()
    pub.close()
    loop.close()

  test "bench_reqrep":
    const N = 5000
    const PayloadLen = 128
    var payload = newString(PayloadLen)
    for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
    var replies = 0
    let loop = newLoop()
    let port = allocPort()
    let rep = newRepSocket(loop)
    rep.`bind`("127.0.0.1", port)
    let req = newReqSocket(loop)
    rep.onRequest = proc(data: openArray[byte]) {.closure.} =
      rep.send(payload)
    req.onReply = proc(data: openArray[byte]) {.closure.} =
      inc replies
      if replies < N:
        req.send(payload)
      else:
        loop.stop()
    var t0 = monoUs()
    discard loop.addTimer(50) do (id: int):
      req.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      req.send(payload)
    loop.run()
    let elapsed = monoUs() - t0
    echo &"  bench_reqrep  N={N}  size={PayloadLen}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
    req.close()
    rep.close()
    loop.close()

  test "bench_sizes_pair":
    for size in [64, 1024, 65536]:
      const N = 2000
      var payload = newString(size)
      for i in 0 ..< size: payload[i] = byte((i and 0x7F) + 32).char
      var received = 0
      var t0 = 0i64
      let loop = newLoop()
      let port = allocPort()
      let srv = newPairSocket(loop)
      srv.`bind`("127.0.0.1", port)
      let cli = newPairSocket(loop)
      srv.onMessage = proc(data: openArray[byte]) {.closure.} =
        inc received
        if received >= N: loop.stop()
      discard loop.addTimer(50) do (id: int):
        cli.connect("127.0.0.1", port)
      discard loop.addTimer(500) do (id: int):
        t0 = monoUs()
        for i in 0 ..< N:
          cli.send(payload)
      loop.run()
      let elapsed = monoUs() - t0
      echo &"  bench_sizes_pair  N={N}  size={size}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
      cli.close()
      srv.close()
      loop.close()

  test "bench_pushpull_throughput":
    const N = 10000
    const PayloadLen = 512
    var payload = newString(PayloadLen)
    for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let push = newPushSocket(loop)
    push.`bind`("127.0.0.1", port)
    let pull = newPullSocket(loop)
    pull.onMessage = proc(data: openArray[byte]) {.closure.} =
      inc received
      if received >= N: loop.stop()
    var t0 = monoUs()
    discard loop.addTimer(50) do (id: int):
      pull.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      for i in 0 ..< N:
        push.send(payload)
    loop.run()
    let elapsed = monoUs() - t0
    echo &"  bench_pushpull_throughput  N={N}  size={PayloadLen}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
    pull.close()
    push.close()
    loop.close()

  test "bench_pushpull_1worker":
    const N = 10000
    const PayloadLen = 128
    var payload = newString(PayloadLen)
    for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let push = newPushSocket(loop)
    push.`bind`("127.0.0.1", port)
    let pull = newPullSocket(loop)
    pull.onMessage = proc(data: openArray[byte]) {.closure.} =
      inc received
      if received >= N: loop.stop()
    discard loop.addTimer(50) do (id: int):
      pull.connect("127.0.0.1", port)
    var t0 = monoUs()
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      for i in 0 ..< N:
        push.send(payload)
    loop.run()
    let elapsed = monoUs() - t0
    echo &"  bench_pushpull_1worker  N={N}  size={PayloadLen}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
    pull.close()
    push.close()
    loop.close()

  test "bench_pushpull_3workers":
    const N = 5000
    const PayloadLen = 128
    var payload = newString(PayloadLen)
    for i in 0 ..< PayloadLen: payload[i] = byte((i and 0x7F) + 32).char
    var received: array[3, int]
    let loop = newLoop()
    let port = allocPort()
    let push = newPushSocket(loop)
    push.`bind`("127.0.0.1", port)
    var pulls: array[3, PullSocket]
    for i in 0 ..< 3:
      pulls[i] = newPullSocket(loop)
      pulls[i].onMessage = proc(data: openArray[byte]) {.closure.} =
        inc received[i]
        var total = 0
        for j in 0 ..< 3: total += received[j]
        if total >= N: loop.stop()
    discard loop.addTimer(50) do (id: int):
      for i in 0 ..< 3:
        pulls[i].connect("127.0.0.1", port)
    var t0 = monoUs()
    discard loop.addTimer(500) do (id: int):
      t0 = monoUs()
      for i in 0 ..< N:
        push.send(payload)
    loop.run()
    let elapsed = monoUs() - t0
    echo &"  bench_pushpull_3workers  N={N}  size={PayloadLen}  {elapsed}us  {(N * 1_000_000 div max(elapsed, 1))}/s"
    for i in 0 ..< 3: pulls[i].close()
    push.close()
    loop.close()



