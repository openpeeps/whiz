## tests/test_clientserver.nim — Tests for the CLIENT/SERVER socket pattern.

import whiz/clientserver
import whiz/pair
import std/[unittest, os]

var nextPort = 29000
proc allocPort: int = result = nextPort; inc nextPort

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

suite "clientserver":

  test "test_clientserver_basic":
    var replied = ""
    let loop = newLoop()
    let port = allocPort()
    let srv = newServerSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newClientSocket(loop)
    srv.onRequest = proc(client: ServerClient; data: openArray[byte]) {.closure.} =
      discard srv.sendTo(client, "reply:" & toStr(data))
    cli.onReply = proc(data: openArray[byte]) {.closure.} =
      replied = toStr(data)
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      discard cli.send("hello")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check replied == "reply:hello"
    check srv.peerCount == 1
    cli.close()
    srv.close()
    loop.close()

  test "test_clientserver_routing":
    ## Two CLIENTs, staggered connects — each reply lands on its own client.
    var cli1Msgs, cli2Msgs: seq[string]
    let loop = newLoop()
    let port = allocPort()
    let srv = newServerSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli1 = newClientSocket(loop)
    let cli2 = newClientSocket(loop)
    srv.onRequest = proc(client: ServerClient; data: openArray[byte]) {.closure.} =
      # Route by numeric id once, by handle once.
      if toStr(data) == "from1":
        discard srv.sendToId(client.id, "to1")
      else:
        discard srv.sendTo(client, "to2")
    cli1.onReply = proc(data: openArray[byte]) {.closure.} =
      cli1Msgs.add(toStr(data))
    cli2.onReply = proc(data: openArray[byte]) {.closure.} =
      cli2Msgs.add(toStr(data))
    discard loop.addTimer(50) do (id: int):
      cli1.connect("127.0.0.1", port)
    discard loop.addTimer(150) do (id: int):
      cli2.connect("127.0.0.1", port)
    discard loop.addTimer(350) do (id: int):
      discard cli1.send("from1")
      discard cli2.send("from2")
    discard loop.addTimer(650) do (id: int):
      loop.stop()
    loop.run()
    check cli1Msgs == @["to1"]
    check cli2Msgs == @["to2"]
    check srv.peerCount == 2
    cli1.close()
    cli2.close()
    srv.close()
    loop.close()

  test "test_clientserver_multipart":
    var gotFrames: seq[string]
    var gotReply: seq[string]
    let loop = newLoop()
    let port = allocPort()
    let srv = newServerSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newClientSocket(loop)
    srv.onRequestMultipart = proc(client: ServerClient; frames: seq[seq[byte]]) {.closure.} =
      gotFrames.setLen(0)
      for f in frames: gotFrames.add(toStr(f))
      discard srv.sendToMultipart(client, ["r1", "", "r3"])
    cli.onReplyMultipart = proc(frames: seq[seq[byte]]) {.closure.} =
      gotReply.setLen(0)
      for f in frames: gotReply.add(toStr(f))
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(250) do (id: int):
      discard cli.sendMultipart(["a", "", "c"])
    discard loop.addTimer(550) do (id: int):
      loop.stop()
    loop.run()
    check gotFrames == @["a", "", "c"]
    check gotReply == @["r1", "", "r3"]
    cli.close()
    srv.close()
    loop.close()

  test "test_clientserver_type_mismatch":
    ## A PAIR peer is rejected; the server drops it.
    let loop = newLoop()
    let port = allocPort()
    let srv = newServerSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let bad = newPairSocket(loop)
    discard loop.addTimer(50) do (id: int):
      bad.connect("127.0.0.1", port)
    discard loop.addTimer(450) do (id: int):
      loop.stop()
    loop.run()
    check srv.peerCount == 0
    bad.close()
    srv.close()
    loop.close()

  test "test_clientserver_close_removes_peer":
    var serverClosed = false
    let loop = newLoop()
    let port = allocPort()
    let srv = newServerSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newClientSocket(loop)
    srv.onClose = proc() {.closure.} =
      serverClosed = true
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(250) do (id: int):
      check srv.peerCount == 1
      cli.close()
    discard loop.addTimer(450) do (id: int):
      loop.stop()
    loop.run()
    check srv.peerCount == 0
    check serverClosed
    srv.close()
    loop.close()

  when not defined(windows):
    test "test_clientserver_unix":
      var replied = ""
      let loop = newLoop()
      let path = "/tmp/whiz-cs-test.sock"
      try: removeFile(path) except OSError: discard
      let srv = newServerSocket(loop)
      srv.`bind`(path, transport = TransportUnix)
      let cli = newClientSocket(loop)
      srv.onRequest = proc(client: ServerClient; data: openArray[byte]) {.closure.} =
        discard srv.sendTo(client, "ux:" & toStr(data))
      cli.onReply = proc(data: openArray[byte]) {.closure.} =
        replied = toStr(data)
      discard loop.addTimer(50) do (id: int):
        cli.connect(path, transport = TransportUnix)
      discard loop.addTimer(200) do (id: int):
        discard cli.send("hello")
      discard loop.addTimer(500) do (id: int):
        loop.stop()
      loop.run()
      check replied == "ux:hello"
      cli.close()
      srv.close()
      loop.close()
      try: removeFile(path) except OSError: discard
