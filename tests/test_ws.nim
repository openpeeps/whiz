## tests/test_ws.nim — Tests for WebSocket transport.

import whiz/ws
import std/unittest

var nextPort = 25000
proc allocPort: int = result = nextPort; inc nextPort

suite "ws_pair":

  test "test_ws_pair_basic":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let srv = newWsPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newWsPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(100) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      cli.send("hello")
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    cli.close()
    srv.close()
    loop.close()

  test "test_ws_pair_bidirectional":
    var srvMsg, cliMsg = 0
    let loop = newLoop()
    let port = allocPort()
    let srv = newWsPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newWsPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      srvMsg += 1
    cli.onMessage = proc(data: openArray[byte]) {.closure.} =
      cliMsg += 1
    discard loop.addTimer(100) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      srv.send("from server")
      cli.send("from client")
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check srvMsg == 1
    check cliMsg == 1
    cli.close()
    srv.close()
    loop.close()

  test "test_ws_pair_close_callback":
    var closed = false
    let loop = newLoop()
    let port = allocPort()
    let srv = newWsPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newWsPairSocket(loop)
    cli.onClose = proc() {.closure.} =
      closed = true
    discard loop.addTimer(100) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      cli.close()
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check closed
    srv.close()
    loop.close()

suite "ws_pubsub":

  test "test_ws_pubsub_basic":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let pub = newWsPubSocket(loop)
    pub.`bind`("127.0.0.1", port)
    let sub = newWsSubSocket(loop)
    sub.onMessage = proc(topic: openArray[byte]; data: openArray[byte]) {.closure.} =
      received += 1
    sub.subscribe("")
    discard loop.addTimer(100) do (id: int):
      sub.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      pub.publish("hello")
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    sub.close()
    pub.close()
    loop.close()

suite "ws_reqrep":

  test "test_ws_reqrep_basic":
    var replyData = ""
    var requestData = ""
    let loop = newLoop()
    let port = allocPort()
    let rep = newWsRepSocket(loop)
    rep.`bind`("127.0.0.1", port)
    let req = newWsReqSocket(loop)
    rep.onRequest = proc(data: openArray[byte]) {.closure.} =
      requestData = cast[string](@data)
      rep.send("reply: " & requestData)
    req.onReply = proc(data: openArray[byte]) {.closure.} =
      replyData = cast[string](@data)
    discard loop.addTimer(100) do (id: int):
      req.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      req.send("hello")
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check requestData == "hello"
    check replyData == "reply: hello"
    req.close()
    rep.close()
    loop.close()

suite "ws_pushpull":

  test "test_ws_pushpull_basic":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let pull = newWsPullSocket(loop)
    pull.`bind`("127.0.0.1", port)
    let push = newWsPushSocket(loop)
    pull.onMessage = proc(data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(100) do (id: int):
      push.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      push.send("hello")
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    push.close()
    pull.close()
    loop.close()
