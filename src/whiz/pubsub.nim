## Publish-Subscribe messaging built on ZMTP 3.0 and powpow TCP.
##
## Provides PUB (publisher) and SUB (subscriber) socket types
## compatible with ZeroMQ's pub-sub pattern:
##
##   - PUB socket binds, accepts connections from SUB peers
##   - SUB socket connects to PUB peers, sends topic subscriptions
##   - Messages are prefix-matched against subscriber topics
##   - Filtering happens at the publisher side
##
## Also provides in-process pub/sub via TopicHub for same-process
## message passing without TCP overhead.
##
## Usage:
##   ```nim
##   # Publisher
##   let pub = newPubSocket(loop, "0.0.0.0", 5555)
##   pub.publish("weather", "sunny")
##
##   # Subscriber
##   let sub = newSubSocket(loop)
##   sub.connect("127.0.0.1", 5555)
##   sub.subscribe("weather")
##   sub.onMessage = proc(topic, data: string) {.closure.} =
##     echo topic, ": ", data
##   ```

import std/[tables, sequtils, strutils]
import powpow/[loop, types, net/tcp]
import ./zmtp

export tcp, loop, types, zmtp

# ── TopicHub (in-process pub/sub) ──────────────────────────────────────────

type
  TopicHandler* = proc(topic: openArray[byte]; data: openArray[byte]) {.closure.}

  TopicHub* = ref object
    loop: Loop
    subs: Table[string, seq[TopicHandler]]

proc newTopicHub*(loop: Loop): TopicHub =
  TopicHub(loop: loop, subs: initTable[string, seq[TopicHandler]]())

proc subscribe*(hub: TopicHub; topic: string; handler: TopicHandler) =
  if hub.subs.hasKey(topic):
    hub.subs[topic].add(handler)
  else:
    hub.subs[topic] = @[handler]

proc unsubscribe*(hub: TopicHub; topic: string; handler: TopicHandler) =
  if hub.subs.hasKey(topic):
    hub.subs[topic].keepItIf(it != handler)

proc publish*(hub: TopicHub; topic: string; data: string) =
  for t, handlers in hub.subs:
    if topic.startsWith(t):
      let t2 = topic
      let d2 = data
      for i in 0 ..< handlers.len:
        let h = handlers[i]
        hub.loop.deferCall(proc() =
          h(t2.toOpenArrayByte(0, t2.high), d2.toOpenArrayByte(0, d2.high)))

# ── Subscriber (per-connection state for PUB socket) ─────────────────────────

type
  Subscriber* = ref object
    zc: ZmtpConnection
    topics: seq[string]

  PubSocket* = ref object
    loop: Loop
    server: TcpServer
    subs: seq[Subscriber]

  SubSocket* = ref object
    loop: Loop
    pubConns: seq[ZmtpConnection]
    subscriptions: seq[string]
    onMessage*: proc(topic: openArray[byte]; data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}

# ── PubSocket ────────────────────────────────────────────────────────────────

proc newPubSocket*(loop: Loop; address: string; port: int = 0;
                   transport: Transport = TransportTcp): PubSocket =
  result = PubSocket(loop: loop, subs: @[])
  let ps = result

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    let zc = initZmtpConnection(conn, asServer = true)
    zc.socketType = "PUB"
    conn.data = cast[pointer](zc)
    var sub = Subscriber(zc: zc, topics: @[])

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "SUB":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()

    zc.onSubscribe = proc(zc: ZmtpConnection; topic: openArray[byte]) =
      let s = newString(topic.len)
      if topic.len > 0:
        copyMem(addr s[0], addr topic[0], topic.len)
      if s notin sub.topics:
        sub.topics.add(s)
    zc.onUnsubscribe = proc(zc: ZmtpConnection; topic: openArray[byte]) =
      let s = newString(topic.len)
      if topic.len > 0:
        copyMem(addr s[0], addr topic[0], topic.len)
      sub.topics.keepItIf(it != s)
    zc.onClose = proc(zc: ZmtpConnection) =
      ps.subs.keepItIf(it != sub)

    ps.subs.add(sub)

  result.server = newTcpServer(loop, onAccept = onAccept, onData = feedData)
  case transport
  of TransportTcp:
    result.server.listen(address, port)
  of TransportUnix:
    when not defined(windows):
      result.server.listenUnix(address)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

proc close*(pub: PubSocket) =
  for sub in pub.subs:
    sub.zc.close()
  pub.subs.setLen(0)
  if pub.server != nil:
    pub.server.close()

proc publish*(pub: PubSocket; data: string) =
  for sub in pub.subs:
    if sub.topics.len > 0:
      for t in sub.topics:
        if t.len == 0 or data.startsWith(t):
          sub.zc.sendMessage(data.toOpenArrayByte(0, data.high))
          break

# ── SubSocket ────────────────────────────────────────────────────────────────

proc newSubSocket*(loop: Loop): SubSocket =
  SubSocket(loop: loop, pubConns: @[], subscriptions: @[])

proc connect*(sub: SubSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  proc onConnect(conn: Connection) =
    let zc = initZmtpConnection(conn, asServer = false)
    zc.socketType = "SUB"
    conn.data = cast[pointer](zc)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PUB":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return
      for s in sub.subscriptions:
        var frame = newSeq[byte](1 + s.len)
        frame[0] = 1
        if s.len > 0:
          copyMem(addr frame[1], addr s[0], s.len)
        discard zc.sendCommand("SUBSCRIBE", frame)

    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if sub.onMessage != nil:
        sub.onMessage(data, data)

    zc.onClose = proc(zc: ZmtpConnection) =
      sub.pubConns.keepItIf(it != zc)
      if sub.onClose != nil: sub.onClose()

    sub.pubConns.add(zc)

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onClose(conn: Connection) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil and zc.onClose != nil:
      zc.state = ZmtpClosed
      zc.onClose(zc)

  case transport
  of TransportTcp:
    sub.loop.connect(address, port, onConnect, feedData, onClose)
  of TransportUnix:
    when not defined(windows):
      sub.loop.connectUnix(address, onConnect, feedData, onClose)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

proc close*(sub: SubSocket) =
  for zc in sub.pubConns:
    zc.close()
  sub.pubConns.setLen(0)

proc subscribe*(sub: SubSocket; topic: string) =
  if topic notin sub.subscriptions:
    sub.subscriptions.add(topic)
  for zc in sub.pubConns:
    if zc.state == ZmtpEstablished:
      var frame = newSeq[byte](1 + topic.len)
      frame[0] = 1
      if topic.len > 0:
        copyMem(addr frame[1], addr topic[0], topic.len)
      discard zc.sendCommand("SUBSCRIBE", frame)

proc unsubscribe*(sub: SubSocket; topic: string) =
  sub.subscriptions.keepItIf(it != topic)
  for zc in sub.pubConns:
    if zc.state == ZmtpEstablished:
      var frame = newSeq[byte](1 + topic.len)
      frame[0] = 0
      if topic.len > 0:
        copyMem(addr frame[1], addr topic[0], topic.len)
      discard zc.sendCommand("UNSUBSCRIBE", frame)
