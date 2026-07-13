## tests/test_reqrep.nim — Tests for the REQ/REP socket pattern.

import whiz/reqrep
import std/[unittest, os]

var nextPort = 25000
proc allocPort: int = result = nextPort; inc nextPort

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

suite "reqrep":

  test "test_reqrep_basic":
    var reply = ""
    let loop = newLoop()
    let port = allocPort()
    let rep = newRepSocket(loop)
    rep.`bind`("127.0.0.1", port)
    let req = newReqSocket(loop)
    rep.onRequest = proc(data: openArray[byte]) {.closure.} =
      rep.send("echo: " & toStr(data))
    req.onReply = proc(data: openArray[byte]) {.closure.} =
      reply = toStr(data)
    discard loop.addTimer(50) do (id: int):
      req.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      req.send("hello")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check reply == "echo: hello"
    req.close()
    rep.close()
    loop.close()

  test "test_reqrep_multiple":
    var replies: seq[string]
    let loop = newLoop()
    let port = allocPort()
    let rep = newRepSocket(loop)
    rep.`bind`("127.0.0.1", port)
    let req = newReqSocket(loop)
    rep.onRequest = proc(data: openArray[byte]) {.closure.} =
      rep.send("reply: " & toStr(data))
    req.onReply = proc(data: openArray[byte]) {.closure.} =
      replies.add(toStr(data))
    discard loop.addTimer(50) do (id: int):
      req.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      req.send("one")
    discard loop.addTimer(300) do (id: int):
      req.send("two")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check replies.len == 2
    check replies[0] == "reply: one"
    check replies[1] == "reply: two"
    req.close()
    rep.close()
    loop.close()

  test "test_reqrep_strict_alternation":
    var replyCount = 0
    let loop = newLoop()
    let port = allocPort()
    let rep = newRepSocket(loop)
    rep.`bind`("127.0.0.1", port)
    let req = newReqSocket(loop)
    rep.onRequest = proc(data: openArray[byte]) {.closure.} =
      rep.send("resp")
    req.onReply = proc(data: openArray[byte]) {.closure.} =
      replyCount += 1
      if replyCount < 3:
        req.send("next")
    discard loop.addTimer(50) do (id: int):
      req.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      req.send("first")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check replyCount == 3
    req.close()
    rep.close()
    loop.close()

  when not defined(windows):
    test "test_reqrep_unix":
      var reply = ""
      let loop = newLoop()
      let path = "/tmp/powpow_test_reqrep.sock"
      removeFile(path)
      let rep = newRepSocket(loop)
      rep.`bind`(path, transport = TransportUnix)
      let req = newReqSocket(loop)
      rep.onRequest = proc(data: openArray[byte]) {.closure.} =
        rep.send("echo: " & toStr(data))
      req.onReply = proc(data: openArray[byte]) {.closure.} =
        reply = toStr(data)
      discard loop.addTimer(50) do (id: int):
        req.connect(path, transport = TransportUnix)
      discard loop.addTimer(200) do (id: int):
        req.send("hello")
      discard loop.addTimer(500) do (id: int):
        loop.stop()
      loop.run()
      check reply == "echo: hello"
      req.close()
      rep.close()
      loop.close()
      removeFile(path)
