## tests/test_pubsub.nim — Tests for ZMTP 3.0 wire protocol and Pub/Sub messaging.

import whiz/pubsub
import std/[unittest, os]

var nextPort = 21000
proc allocPort: int = result = nextPort; inc nextPort

suite "inproc_pubsub":

  test "test_inproc_basic":
    var received = 0
    let loop = newLoop()
    let hub = newTopicHub(loop)
    hub.subscribe("chat") do (topic, data: openArray[byte]):
      received += 1
    discard loop.addTimer(10) do (id: int):
      hub.publish("chat.hello", "world")
    discard loop.addTimer(50) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    loop.close()

  test "test_inproc_topic_match":
    var chatCount, alertCount = 0
    let loop = newLoop()
    let hub = newTopicHub(loop)
    hub.subscribe("chat") do (topic, data: openArray[byte]): inc chatCount
    hub.subscribe("alert") do (topic, data: openArray[byte]): inc alertCount
    discard loop.addTimer(10) do (id: int):
      hub.publish("chat.hello", "hi")
      hub.publish("alert.critical", "fire")
    discard loop.addTimer(50) do (id: int):
      loop.stop()
    loop.run()
    check chatCount == 1
    check alertCount == 1
    loop.close()

  test "test_inproc_unsubscribe":
    var count = 0
    let loop = newLoop()
    let hub = newTopicHub(loop)
    var handler: TopicHandler
    handler = proc(topic, data: openArray[byte]) {.closure.} =
      inc count
      hub.unsubscribe("test", handler)
    hub.subscribe("test", handler)
    discard loop.addTimer(10) do (id: int):
      hub.publish("test.a", "1")
    discard loop.addTimer(30) do (id: int):
      hub.publish("test.b", "2")
    discard loop.addTimer(50) do (id: int):
      loop.stop()
    loop.run()
    check count == 1
    loop.close()

suite "tcp_pubsub":

  test "test_pubsub_basic":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let pub = newPubSocket(loop, "127.0.0.1", port)
    let sub = newSubSocket(loop)
    sub.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(50) do (id: int):
      sub.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      sub.subscribe("")
    discard loop.addTimer(350) do (id: int):
      pub.publish("hello world")
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    pub.close()
    sub.close()
    loop.close()

  test "test_pubsub_topic_filter":
    var msgs: seq[string]
    let loop = newLoop()
    let port = allocPort()
    let pub = newPubSocket(loop, "127.0.0.1", port)
    let sub = newSubSocket(loop)
    sub.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
      var s = newString(data.len)
      if data.len > 0:
        copyMem(addr s[0], unsafeAddr data[0], data.len)
      msgs.add(s)
    discard loop.addTimer(50) do (id: int):
      sub.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      sub.subscribe("foo")
    discard loop.addTimer(400) do (id: int):
      pub.publish("foo message")
      pub.publish("bar message")
    discard loop.addTimer(700) do (id: int):
      loop.stop()
    loop.run()
    check msgs.len == 1
    check msgs[0] == "foo message"
    pub.close()
    sub.close()
    loop.close()

  test "test_pubsub_multiple_subs":
    var a, b = 0
    let loop = newLoop()
    let port = allocPort()
    let pub = newPubSocket(loop, "127.0.0.1", port)
    let sub1 = newSubSocket(loop)
    let sub2 = newSubSocket(loop)
    sub1.onMessage = proc(topic, data: openArray[byte]) {.closure.} = a += 1
    sub2.onMessage = proc(topic, data: openArray[byte]) {.closure.} = b += 1
    discard loop.addTimer(50) do (id: int):
      sub1.connect("127.0.0.1", port)
      sub2.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      sub1.subscribe("")
      sub2.subscribe("")
    discard loop.addTimer(400) do (id: int):
      pub.publish("broadcast")
    discard loop.addTimer(700) do (id: int):
      loop.stop()
    loop.run()
    check a == 1
    check b == 1
    pub.close()
    sub1.close()
    sub2.close()
    loop.close()

  test "test_pubsub_unsubscribe":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let pub = newPubSocket(loop, "127.0.0.1", port)
    let sub = newSubSocket(loop)
    sub.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(50) do (id: int):
      sub.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      sub.subscribe("msg")
    discard loop.addTimer(350) do (id: int):
      pub.publish("msg first")
    discard loop.addTimer(500) do (id: int):
      sub.unsubscribe("msg")
    discard loop.addTimer(700) do (id: int):
      pub.publish("msg second")
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    pub.close()
    sub.close()
    loop.close()

  when not defined(windows):
    test "test_pubsub_unix":
      var received = 0
      let loop = newLoop()
      let path = "/tmp/powpow_test_pubsub.sock"
      removeFile(path)
      let pub = newPubSocket(loop, path, transport = TransportUnix)
      let sub = newSubSocket(loop)
      sub.onMessage = proc(topic, data: openArray[byte]) {.closure.} =
        received += 1
      discard loop.addTimer(50) do (id: int):
        sub.connect(path, transport = TransportUnix)
      discard loop.addTimer(300) do (id: int):
        sub.subscribe("")
      discard loop.addTimer(350) do (id: int):
        pub.publish("hello world")
      discard loop.addTimer(500) do (id: int):
        loop.stop()
      loop.run()
      check received == 1
      pub.close()
      sub.close()
      loop.close()
      removeFile(path)
