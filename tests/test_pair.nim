## tests/test_pair.nim — Tests for the PAIR socket pattern.

import whiz/pair
import std/[unittest, os]

var nextPort = 24000
proc allocPort: int = result = nextPort; inc nextPort

suite "pair":

  test "test_pair_basic":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      cli.send("hello")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    cli.close()
    srv.close()
    loop.close()

  test "test_pair_bidirectional":
    var srvMsg, cliMsg = 0
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      srvMsg += 1
    cli.onMessage = proc(data: openArray[byte]) {.closure.} =
      cliMsg += 1
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      srv.send("from server")
      cli.send("from client")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check srvMsg == 1
    check cliMsg == 1
    cli.close()
    srv.close()
    loop.close()

  test "test_pair_close_callback":
    var closed = false
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    cli.onClose = proc() {.closure.} =
      closed = true
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      cli.close()
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check closed
    srv.close()
    loop.close()

  when not defined(windows):
    test "test_pair_unix":
      var received = 0
      let loop = newLoop()
      let path = "/tmp/powpow_test_pair.sock"
      removeFile(path)
      let srv = newPairSocket(loop)
      srv.`bind`(path, transport = TransportUnix)
      let cli = newPairSocket(loop)
      srv.onMessage = proc(data: openArray[byte]) {.closure.} =
        received += 1
      discard loop.addTimer(50) do (id: int):
        cli.connect(path, transport = TransportUnix)
      discard loop.addTimer(200) do (id: int):
        cli.send("hello")
      discard loop.addTimer(500) do (id: int):
        loop.stop()
      loop.run()
      check received == 1
      cli.close()
      srv.close()
      loop.close()
      removeFile(path)
