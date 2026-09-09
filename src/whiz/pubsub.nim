# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## Publish-Subscribe messaging built on ZMTP 3.0 and powpow TCP.

import std/[tables, sequtils, strutils]
import powpow/[loop, types, net/tcp]
import ./zmtp
import ./auth
import ./curve
import ./tls

export tcp, loop, types, zmtp, auth, curve, tls

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
    authMech: string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    tls: TlsConfig

  SubSocket* = ref object
    loop: Loop
    pubConns: seq[ZmtpConnection]
    subscriptions: seq[string]
    onMessage*: proc(topic: openArray[byte]; data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}
    authMech: string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    tls: TlsConfig

proc setCurveKeypair*(pub: PubSocket; publicKey, secretKey: array[32, uint8]) =
  pub.authMech = "CURVE"
  pub.authPubKey = publicKey
  pub.authSecKey = secretKey

proc setCurveClient*(sub: SubSocket; publicKey, secretKey, serverKey: array[32, uint8]) =
  sub.authMech = "CURVE"
  sub.authPubKey = publicKey
  sub.authSecKey = secretKey
  sub.authSrvKey = serverKey

proc setTlsServer*(s: PubSocket | SubSocket; certFile, keyFile: string) =
  ## Enables TLS for accepted connections (TCP only, POSIX only). Call
  ## before bind. Raises SslError when the cert and key do not match.
  enableTlsServer(s.tls, certFile, keyFile)

proc setTlsClient*(s: PubSocket | SubSocket; verifyPeer = true; serverName = "") =
  ## Enables TLS for outbound connections (TCP only, POSIX only). Call
  ## before connect. Pass verifyPeer=false for self-signed test certs.
  enableTlsClient(s.tls, verifyPeer, serverName)

# ── PubSocket ────────────────────────────────────────────────────────────────

proc newPubSocket*(loop: Loop; address: string; port: int = 0;
                   transport: Transport = TransportTcp): PubSocket =
  result = PubSocket(loop: loop, subs: @[])
  let ps = result

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    if transport == TransportTcp and not wrapServerConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "PUB"
    conn.data = cast[pointer](zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
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

  proc onConnClosed(conn: Connection) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil and zc.state != ZmtpClosed:
      zc.state = ZmtpClosed
      if zc.onClose != nil: zc.onClose(zc)

  result.server = newTcpServer(loop, onAccept = onAccept, onData = feedData,
                               onClose = onConnClosed)
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

proc publish*(pub: PubSocket; data: string): bool {.discardable.} =
  ## Sends to every subscriber with a matching topic. Returns true when no
  ## matching send failed (vacuously true when nothing matched); false means
  ## at least one matched subscriber's transport is dead and that copy was
  ## not delivered.
  result = true
  for sub in pub.subs:
    if sub.topics.len > 0:
      for t in sub.topics:
        if t.len == 0 or data.startsWith(t):
          if not sub.zc.sendMessage(data.toOpenArrayByte(0, data.high)):
            result = false
          break

# ── SubSocket ────────────────────────────────────────────────────────────────

proc newSubSocket*(loop: Loop): SubSocket =
  SubSocket(loop: loop, pubConns: @[], subscriptions: @[])

proc connect*(sub: SubSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  proc onConnect(conn: Connection) =
    if transport == TransportTcp and not wrapClientConn(conn, sub.tls): return
    let mech = if sub.authMech.len > 0: sub.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "SUB"
    conn.data = cast[pointer](zc)
    if sub.authMech == "CURVE":
      curve.setCurveKeypair(zc, sub.authPubKey, sub.authSecKey)
      if sub.authSrvKey != default(array[32, uint8]):
        curve.setCurveServerKey(zc, sub.authSrvKey)

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
