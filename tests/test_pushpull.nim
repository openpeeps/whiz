## tests/test_pushpull.nim — Tests for the PUSH/PULL socket pattern.

import whiz/pushpull
import std/[unittest, os]

var nextPort = 28000
proc allocPort: int = result = nextPort; inc nextPort

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

suite "pushpull":

  test "test_pushpull_basic":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let pull = newPullSocket(loop)
    pull.`bind`("127.0.0.1", port)
    let push = newPushSocket(loop)
    pull.onMessage = proc(data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(50) do (id: int):
      push.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      push.send("hello")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    push.close()
    pull.close()
    loop.close()

  test "test_pushpull_round_robin":
    ## PUSH binds, multiple PULLs connect — messages round-robin to each PULL
    var pull1Msgs, pull2Msgs: seq[string]
    let loop = newLoop()
    let port = allocPort()
    let push = newPushSocket(loop)
    push.`bind`("127.0.0.1", port)
    let pull1 = newPullSocket(loop)
    let pull2 = newPullSocket(loop)
    pull1.onMessage = proc(data: openArray[byte]) {.closure.} =
      pull1Msgs.add(toStr(data))
    pull2.onMessage = proc(data: openArray[byte]) {.closure.} =
      pull2Msgs.add(toStr(data))
    discard loop.addTimer(50) do (id: int):
      pull1.connect("127.0.0.1", port)
    discard loop.addTimer(150) do (id: int):
      pull2.connect("127.0.0.1", port)
    discard loop.addTimer(350) do (id: int):
      push.send("msg1")
      push.send("msg2")
    discard loop.addTimer(600) do (id: int):
      loop.stop()
    loop.run()
    check pull1Msgs.len == 1
    check pull2Msgs.len == 1
    pull1.close()
    pull2.close()
    push.close()
    loop.close()

  test "test_pushpull_multiple_senders":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let pull = newPullSocket(loop)
    pull.`bind`("127.0.0.1", port)
    let push1 = newPushSocket(loop)
    let push2 = newPushSocket(loop)
    pull.onMessage = proc(data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(50) do (id: int):
      push1.connect("127.0.0.1", port)
      push2.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      push1.send("from push1")
      push2.send("from push2")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check received == 2
    push1.close()
    push2.close()
    pull.close()
    loop.close()

  test "test_pushpull_close_callback":
    var closed = false
    let loop = newLoop()
    let port = allocPort()
    let pull = newPullSocket(loop)
    pull.`bind`("127.0.0.1", port)
    let push = newPushSocket(loop)
    push.onClose = proc() {.closure.} =
      closed = true
    discard loop.addTimer(50) do (id: int):
      push.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      push.close()
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check closed
    pull.close()
    loop.close()

  when not defined(windows):
    test "test_pushpull_unix":
      var received = 0
      let loop = newLoop()
      let path = "/tmp/zap_test_pushpull.sock"
      removeFile(path)
      let pull = newPullSocket(loop)
      pull.`bind`(path, transport = TransportUnix)
      let push = newPushSocket(loop)
      pull.onMessage = proc(data: openArray[byte]) {.closure.} =
        received += 1
      discard loop.addTimer(50) do (id: int):
        push.connect(path, transport = TransportUnix)
      discard loop.addTimer(200) do (id: int):
        push.send("hello")
      discard loop.addTimer(500) do (id: int):
        loop.stop()
      loop.run()
      check received == 1
      push.close()
      pull.close()
      loop.close()
      removeFile(path)
