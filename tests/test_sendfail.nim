## tests/test_sendfail.nim — Send-failure surfacing and dead-peer detection.
##
## Socket sends return bool {.discardable.}: false means the message was NOT
## delivered. Server-side sockets learn about peer death through powpow's
## TcpServer onClose wiring, which drives zc.onClose and the socket's user
## onClose callback.

import whiz/pair
import whiz/pubsub
import whiz/reqrep
import whiz/pushpull
import std/unittest

var nextPort = 28100
proc allocPort: int = result = nextPort; inc nextPort

suite "send failure surfacing":

  test "pair_send_fails_before_connect":
    let loop = newLoop()
    let cli = newPairSocket(loop)
    check cli.send("x") == false
    loop.close()

  test "pair_server_detects_dead_client":
    var srvCloseFired = false
    var sendAfterDeath: seq[bool]
    let port = allocPort()
    let loop = newLoop()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      discard srv.send("ack")
    srv.onClose = proc() {.closure.} =
      srvCloseFired = true
    let cli = newPairSocket(loop)
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      # handshake is done; client goes away abruptly (FIN)
      cli.close()
    discard loop.addTimer(600) do (id: int):
      sendAfterDeath.add(srv.send("too late"))
      loop.stop()
    discard loop.addTimer(5000) do (id: int):
      loop.stop()
    loop.run()
    check srvCloseFired
    check sendAfterDeath.len == 1 and sendAfterDeath[0] == false
    cli.close(); srv.close(); loop.close()

  test "pair_write_cap_kill_surfaces_as_failure":
    ## Mirrors the bench hang root cause: a flood that exceeds powpow's
    ## write-buffer cap makes powpow close the conn internally; sends must
    ## report failure instead of silently vanishing.
    const Flood = 700
    const Size = 65536          # ~46MB total, over the default 32MB cap
    var payload = newString(Size)
    for i in 0 ..< Size: payload[i] = byte((i and 0x7F) + 32).char
    var sawFalse = false
    var sentOk = 0
    var srvRecv = 0
    let port = allocPort()
    let loop = newLoop()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      inc srvRecv
    let cli = newPairSocket(loop)
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(400) do (id: int):
      for i in 0 ..< Flood:
        if cli.send(payload): inc sentOk
        else: sawFalse = true
    discard loop.addTimer(1200) do (id: int):
      # after the cap kill, further sends must fail honestly
      if not cli.send("post-kill"): sawFalse = true
      loop.stop()
    discard loop.addTimer(8000) do (id: int):
      loop.stop()
    loop.run()
    check sawFalse
    check sentOk < Flood
    cli.close(); srv.close(); loop.close()

  test "pubsub_publish_vacuous_true_and_bool_path":
    let port = allocPort()
    let loop = newLoop()
    let pub = newPubSocket(loop, "127.0.0.1", port)
    var pubResult: seq[bool]
    let sub = newSubSocket(loop)
    var got = 0
    sub.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
      inc got
    sub.subscribe("")
    pubResult.add(pub.publish("nobody listening"))   # no subs yet: vacuous true
    discard loop.addTimer(50) do (id: int):
      sub.connect("127.0.0.1", port)
    discard loop.addTimer(400) do (id: int):
      pubResult.add(pub.publish("hello"))
    discard loop.addTimer(900) do (id: int):
      loop.stop()
    discard loop.addTimer(5000) do (id: int):
      loop.stop()
    loop.run()
    check pubResult.len == 2
    check pubResult[0] == true       # no matching subscriber = not a failure
    check pubResult[1] == true       # delivered to live subscriber
    check got == 1
    sub.close(); pub.close(); loop.close()

  test "reqrep_alternation_and_preconnect_failures":
    var reply = ""
    let port = allocPort()
    let loop = newLoop()
    let rep = newRepSocket(loop)
    rep.`bind`("127.0.0.1", port)
    rep.onRequest = proc(data: openArray[byte]) {.closure.} =
      check rep.send("pong") == true
      # second reply without a request must fail (strict alternation)
      check rep.send("bogus") == false
    let req = newReqSocket(loop)
    req.onReply = proc(data: openArray[byte]) {.closure.} =
      reply = cast[string](@data)
      loop.stop()
    check req.send("early") == false     # not connected yet
    discard loop.addTimer(50) do (id: int):
      req.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      check req.send("ping") == true
      check req.send("while waiting") == false   # locked until reply
    discard loop.addTimer(5000) do (id: int):
      loop.stop()
    loop.run()
    check reply == "pong"
    req.close(); rep.close(); loop.close()

  test "pushpull_no_worker_is_false":
    let loop = newLoop()
    let push = newPushSocket(loop)
    let pull = newPullSocket(loop)
    check push.send("x") == false        # no workers connected
    check pull.send("x") == false        # no push peers connected
    loop.close()
