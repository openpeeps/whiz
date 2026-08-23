# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## WebSocket transport for ZMTP 3.0. Maps each ZMTP frame to a single WebSocket
## binary message. Powered by powpow's RFC 6455 WebSocket implementation
## (role-aware masking, handshake timeouts, and native `connectWs` client).

import std/[sequtils, strutils]
import powpow/[loop, types, net/tcp, proto/ws]
import ./zmtp
export loop, types, tcp, ws, zmtp

# ── ZMTP ↔ WebSocket bridge ──────────────────────────────────────────────────

proc wsBridge(zc: ZmtpConnection, ws: WsConnection) =
  ## Tunnels ZMTP frames through a WebSocket binary-message pipe.
  zc.sendOverride = proc(zc: ZmtpConnection; data: openArray[byte]): int =
    if ws.conn.state != Connected: return -1
    ws.sendBinary(data)
    data.len
  ws.onMessage = proc(ws: WsConnection; kind: WsFrameKind; data: openArray[byte]) =
    if kind == wsBinary:
      zc.feed(data)
  ws.onClose = proc(ws: WsConnection; code: int; reason: string) =
    if zc.state != ZmtpClosed:
      zc.state = ZmtpClosed
      if zc.onClose != nil:
        zc.onClose(zc)

proc wsSendGreeting(zc: ZmtpConnection, ws: WsConnection) =
  let greeting = buildGreeting(zc.asServer, zc.mechanism)
  ws.sendBinary(greeting)

# ── WsPairSocket ──────────────────────────────────────────────────────────────

type
  WsPairSocket* = ref object
    loop: Loop
    wss: WsServer
    ws: WsConnection
    zc: ZmtpConnection
    conn: Connection
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}

proc newWsPairSocket*(loop: Loop): WsPairSocket =
  WsPairSocket(loop: loop)

proc send*(s: WsPairSocket; data: string): bool {.discardable.} =
  if s.zc != nil and s.zc.state == ZmtpEstablished:
    return s.zc.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc close*(s: WsPairSocket) =
  if s.zc != nil:
    s.zc.close()
    s.zc = nil
  if s.wss != nil:
    s.wss.close()
    s.wss = nil
  if s.onClose != nil: s.onClose()

proc `bind`*(s: WsPairSocket; address: string; port: int) =
  s.wss = newWsServer(s.loop)
  s.wss.onOpen do (ws: WsConnection):
    if s.zc != nil:
      ws.closeWs(1000, "Only one connection allowed")
      return
    let zc = initZmtpConnection(ws.conn, asServer = true, "NULL", skipGreeting = true)
    zc.socketType = "PAIR"
    ws.conn.data = cast[pointer](zc)
    s.ws = ws
    s.zc = zc
    wsBridge(zc, ws)
    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PAIR":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if s.onMessage != nil: s.onMessage(data)
    zc.onClose = proc(zc: ZmtpConnection) =
      s.zc = nil
      s.ws = nil
      if s.onClose != nil: s.onClose()
    wsSendGreeting(zc, ws)
  s.wss.listen(address, port)

proc connect*(s: WsPairSocket; address: string; port: int; path: string = "/") =
  let self = s
  discard connectWs(s.loop, address, port, path,
    onOpen = proc(ws: WsConnection) {.closure.} =
      if self.zc != nil:
        ws.closeWs(1000, "Only one connection allowed")
        return
      let zc = initZmtpConnection(ws.conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "PAIR"
      self.ws = ws
      self.conn = ws.conn
      self.zc = zc
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) {.closure.} =
        if zc.peerSocketType != "PAIR":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
      zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) {.closure.} =
        if self.onMessage != nil: self.onMessage(data)
      zc.onClose = proc(zc: ZmtpConnection) {.closure.} =
        self.zc = nil
        self.ws = nil
        if self.onClose != nil: self.onClose()
      wsSendGreeting(zc, ws),
    onClose = proc(ws: WsConnection; code: int; reason: string) {.closure.} =
      # With an established session the bridge's ws.onClose already drove
      # zc.onClose (which fired the user callback). A death before onOpen
      # means no session ever existed — surface it directly.
      if self.zc == nil and self.onClose != nil:
        self.onClose(),
    onError = proc(ws: WsConnection; err: string) {.closure.} =
      if self.zc != nil and self.zc.onError != nil:
        self.zc.onError(self.zc, err))

# ── WsPubSocket ───────────────────────────────────────────────────────────────

type
  WsSubscriber* = ref object
    zc: ZmtpConnection
    ws: WsConnection
    topics: seq[string]

  WsPubSocket* = ref object
    loop: Loop
    wss: WsServer
    subs: seq[WsSubscriber]

proc newWsPubSocket*(loop: Loop): WsPubSocket =
  WsPubSocket(loop: loop, subs: @[])

proc close*(s: WsPubSocket) =
  for sub in s.subs:
    sub.zc.close()
  s.subs.setLen(0)
  if s.wss != nil:
    s.wss.close()

proc publish*(s: WsPubSocket; data: string): bool {.discardable.} =
  result = true
  for sub in s.subs:
    if sub.topics.len > 0:
      for t in sub.topics:
        if t.len == 0 or data.startsWith(t):
          if not sub.zc.sendMessage(data.toOpenArrayByte(0, data.high)):
            result = false
          break

proc `bind`*(s: WsPubSocket; address: string; port: int) =
  s.wss = newWsServer(s.loop)
  s.wss.onOpen do (ws: WsConnection):
    let zc = initZmtpConnection(ws.conn, asServer = true, "NULL", skipGreeting = true)
    zc.socketType = "PUB"
    ws.conn.data = cast[pointer](zc)
    wsBridge(zc, ws)
    var sub = WsSubscriber(zc: zc, ws: ws, topics: @[])
    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "SUB":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
    zc.onSubscribe = proc(zc: ZmtpConnection; topic: openArray[byte]) =
      let topicStr = newString(topic.len)
      if topic.len > 0:
        copyMem(addr topicStr[0], addr topic[0], topic.len)
      if topicStr notin sub.topics:
        sub.topics.add(topicStr)
    zc.onUnsubscribe = proc(zc: ZmtpConnection; topic: openArray[byte]) =
      let topicStr = newString(topic.len)
      if topic.len > 0:
        copyMem(addr topicStr[0], addr topic[0], topic.len)
      sub.topics.keepItIf(it != topicStr)
    zc.onClose = proc(zc: ZmtpConnection) =
      s.subs.keepItIf(it != sub)
    s.subs.add(sub)
    wsSendGreeting(zc, ws)
  s.wss.listen(address, port)

# ── WsSubSocket ───────────────────────────────────────────────────────────────

type
  WsSubSocket* = ref object
    loop: Loop
    ws: seq[WsConnection]
    zcs: seq[ZmtpConnection]
    subscriptions: seq[string]
    onMessage*: proc(topic: openArray[byte]; data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}

proc newWsSubSocket*(loop: Loop): WsSubSocket =
  WsSubSocket(loop: loop, ws: @[], zcs: @[], subscriptions: @[])

proc close*(s: WsSubSocket) =
  for zc in s.zcs:
    zc.close()
  s.zcs.setLen(0)
  s.ws.setLen(0)

proc subscribe*(s: WsSubSocket; topic: string) =
  if topic notin s.subscriptions:
    s.subscriptions.add(topic)
  for i in 0 ..< s.zcs.len:
    let zc = s.zcs[i]
    if zc.state == ZmtpEstablished:
      var frame = newSeq[byte](1 + topic.len)
      frame[0] = 1
      if topic.len > 0:
        copyMem(addr frame[1], addr topic[0], topic.len)
      discard zc.sendCommand("SUBSCRIBE", frame)

proc unsubscribe*(s: WsSubSocket; topic: string) =
  s.subscriptions.keepItIf(it != topic)
  for zc in s.zcs:
    if zc.state == ZmtpEstablished:
      var frame = newSeq[byte](1 + topic.len)
      frame[0] = 0
      if topic.len > 0:
        copyMem(addr frame[1], addr topic[0], topic.len)
      discard zc.sendCommand("UNSUBSCRIBE", frame)

proc connect*(s: WsSubSocket; address: string; port: int; path: string = "/") =
  let self = s
  var closed = false
  discard connectWs(s.loop, address, port, path,
    onOpen = proc(ws: WsConnection) {.closure.} =
      let zc = initZmtpConnection(ws.conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "SUB"
      self.ws.add(ws)
      self.zcs.add(zc)
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) {.closure.} =
        if zc.peerSocketType != "PUB":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
          return
        for topic in self.subscriptions:
          var frame = newSeq[byte](1 + topic.len)
          frame[0] = 1
          if topic.len > 0:
            copyMem(addr frame[1], addr topic[0], topic.len)
          discard zc.sendCommand("SUBSCRIBE", frame)
      zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) {.closure.} =
        if self.onMessage != nil:
          self.onMessage(data, data)
      zc.onClose = proc(zc: ZmtpConnection) {.closure.} =
        if closed: return
        closed = true
        var idx = -1
        for i in 0 ..< self.zcs.len:
          if self.zcs[i] == zc:
            idx = i
            break
        if idx >= 0:
          self.zcs.delete(idx)
          if idx < self.ws.len: self.ws.delete(idx)
        if self.onClose != nil: self.onClose()
      wsSendGreeting(zc, ws),
    onClose = proc(ws: WsConnection; code: int; reason: string) {.closure.} =
      if closed: return
      closed = true
      if self.onClose != nil: self.onClose(),
    onError = proc(ws: WsConnection; err: string) {.closure.} =
      discard)

# ── WsReqSocket ───────────────────────────────────────────────────────────────

type
  WsReqSocket* = ref object
    loop: Loop
    ws: WsConnection
    zc: ZmtpConnection
    conn: Connection
    waiting: bool
    onReply*: proc(data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}

proc newWsReqSocket*(loop: Loop): WsReqSocket =
  WsReqSocket(loop: loop)

proc send*(s: WsReqSocket; data: string): bool {.discardable.} =
  if s.zc != nil and s.zc.state == ZmtpEstablished and not s.waiting:
    s.waiting = true
    if s.zc.sendMessage(data.toOpenArrayByte(0, data.high)):
      return true
    s.waiting = false
  false

proc close*(s: WsReqSocket) =
  if s.zc != nil:
    s.zc.close()
    s.zc = nil
  if s.onClose != nil: s.onClose()

proc connect*(s: WsReqSocket; address: string; port: int; path: string = "/") =
  let self = s
  discard connectWs(s.loop, address, port, path,
    onOpen = proc(ws: WsConnection) {.closure.} =
      let zc = initZmtpConnection(ws.conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "REQ"
      self.ws = ws
      self.conn = ws.conn
      self.zc = zc
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) {.closure.} =
        if zc.peerSocketType != "REP":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
      zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) {.closure.} =
        self.waiting = false
        if self.onReply != nil:
          self.onReply(data)
      zc.onClose = proc(zc: ZmtpConnection) {.closure.} =
        self.zc = nil
        self.ws = nil
        if self.onClose != nil: self.onClose()
      wsSendGreeting(zc, ws),
    onClose = proc(ws: WsConnection; code: int; reason: string) {.closure.} =
      if self.zc == nil and self.onClose != nil:
        self.onClose(),
    onError = proc(ws: WsConnection; err: string) {.closure.} =
      if self.zc != nil and self.zc.onError != nil:
        self.zc.onError(self.zc, err))

# ── WsRepSocket ───────────────────────────────────────────────────────────────

type
  WsRepSocket* = ref object
    loop: Loop
    wss: WsServer
    ws: WsConnection
    zc: ZmtpConnection
    hasRequest: bool
    onRequest*: proc(data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}

proc newWsRepSocket*(loop: Loop): WsRepSocket =
  WsRepSocket(loop: loop)

proc send*(s: WsRepSocket; data: string): bool {.discardable.} =
  if s.zc != nil and s.zc.state == ZmtpEstablished and s.hasRequest:
    s.hasRequest = false
    if s.zc.sendMessage(data.toOpenArrayByte(0, data.high)):
      return true
    s.hasRequest = true
  false

proc close*(s: WsRepSocket) =
  if s.zc != nil:
    s.zc.close()
    if s.onClose != nil: s.onClose()
    s.zc = nil
  if s.wss != nil:
    s.wss.close()
    s.wss = nil

proc `bind`*(s: WsRepSocket; address: string; port: int) =
  s.wss = newWsServer(s.loop)
  s.wss.onOpen do (ws: WsConnection):
    if s.zc != nil:
      ws.closeWs(1000, "Only one connection allowed")
      return
    let zc = initZmtpConnection(ws.conn, asServer = true, "NULL", skipGreeting = true)
    zc.socketType = "REP"
    ws.conn.data = cast[pointer](zc)
    s.ws = ws
    s.zc = zc
    wsBridge(zc, ws)
    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "REQ":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return
    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if not s.hasRequest:
        s.hasRequest = true
        if s.onRequest != nil:
          s.onRequest(data)
    zc.onClose = proc(zc: ZmtpConnection) =
      s.zc = nil
      s.ws = nil
      if s.onClose != nil: s.onClose()
    wsSendGreeting(zc, ws)
  s.wss.listen(address, port)

# ── WsPushSocket ──────────────────────────────────────────────────────────────

type
  WsPushSocket* = ref object
    loop: Loop
    wss: WsServer
    ws: seq[WsConnection]
    zcs: seq[ZmtpConnection]
    rrIndex: int
    onClose*: proc() {.closure.}

proc newWsPushSocket*(loop: Loop): WsPushSocket =
  WsPushSocket(loop: loop, ws: @[], zcs: @[], rrIndex: 0)

proc send*(s: WsPushSocket; data: string): bool {.discardable.} =
  if s.zcs.len == 0: return false
  if s.rrIndex >= s.zcs.len: s.rrIndex = 0
  let idx = s.rrIndex
  s.rrIndex = (s.rrIndex + 1) mod s.zcs.len
  let zc = s.zcs[idx]
  if zc.state == ZmtpEstablished:
    return zc.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc close*(s: WsPushSocket) =
  for zc in s.zcs:
    zc.close()
  s.zcs.setLen(0)
  s.ws.setLen(0)
  if s.wss != nil:
    s.wss.close()
    s.wss = nil
  if s.onClose != nil: s.onClose()

proc `bind`*(s: WsPushSocket; address: string; port: int) =
  s.wss = newWsServer(s.loop)
  s.wss.onOpen do (ws: WsConnection):
    let zc = initZmtpConnection(ws.conn, asServer = true, "NULL", skipGreeting = true)
    zc.socketType = "PUSH"
    ws.conn.data = cast[pointer](zc)
    wsBridge(zc, ws)
    s.ws.add(ws)
    s.zcs.add(zc)
    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PULL":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        var idx = -1
        for i in 0 ..< s.zcs.len:
          if s.zcs[i] == zc:
            idx = i
            break
        if idx >= 0:
          s.zcs.delete(idx)
          if idx < s.ws.len: s.ws.delete(idx)
    zc.onClose = proc(zc: ZmtpConnection) =
      var idx = -1
      for i in 0 ..< s.zcs.len:
        if s.zcs[i] == zc:
          idx = i
          break
      if idx >= 0:
        s.zcs.delete(idx)
        if idx < s.ws.len: s.ws.delete(idx)
      if s.zcs.len == 0 and s.onClose != nil:
        s.onClose()
    wsSendGreeting(zc, ws)
  s.wss.listen(address, port)

proc connect*(s: WsPushSocket; address: string; port: int; path: string = "/") =
  let self = s
  var closed = false
  discard connectWs(s.loop, address, port, path,
    onOpen = proc(ws: WsConnection) {.closure.} =
      let zc = initZmtpConnection(ws.conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "PUSH"
      self.ws.add(ws)
      self.zcs.add(zc)
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) {.closure.} =
        if zc.peerSocketType != "PULL":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
      zc.onClose = proc(zc: ZmtpConnection) {.closure.} =
        if closed: return
        closed = true
        var idx = -1
        for i in 0 ..< self.zcs.len:
          if self.zcs[i] == zc:
            idx = i
            break
        if idx >= 0:
          self.zcs.delete(idx)
          if idx < self.ws.len: self.ws.delete(idx)
        if self.zcs.len == 0 and self.onClose != nil:
          self.onClose()
      wsSendGreeting(zc, ws),
    onClose = proc(ws: WsConnection; code: int; reason: string) {.closure.} =
      if closed: return
      closed = true
      if self.zcs.len == 0 and self.onClose != nil:
        self.onClose(),
    onError = proc(ws: WsConnection; err: string) {.closure.} =
      discard)

# ── WsPullSocket ──────────────────────────────────────────────────────────────

type
  WsPullSocket* = ref object
    loop: Loop
    wss: WsServer
    ws: seq[WsConnection]
    zcs: seq[ZmtpConnection]
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}

proc newWsPullSocket*(loop: Loop): WsPullSocket =
  WsPullSocket(loop: loop, ws: @[], zcs: @[])

proc send*(s: WsPullSocket; data: string): bool {.discardable.} =
  if s.zcs.len == 0: return false
  let zc = s.zcs[^1]
  if zc.state == ZmtpEstablished:
    return zc.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc close*(s: WsPullSocket) =
  for zc in s.zcs:
    zc.close()
  s.zcs.setLen(0)
  s.ws.setLen(0)
  if s.wss != nil:
    s.wss.close()
    s.wss = nil
  if s.onClose != nil: s.onClose()

proc `bind`*(s: WsPullSocket; address: string; port: int) =
  s.wss = newWsServer(s.loop)
  s.wss.onOpen do (ws: WsConnection):
    let zc = initZmtpConnection(ws.conn, asServer = true, "NULL", skipGreeting = true)
    zc.socketType = "PULL"
    ws.conn.data = cast[pointer](zc)
    wsBridge(zc, ws)
    s.ws.add(ws)
    s.zcs.add(zc)
    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PUSH":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        var idx = -1
        for i in 0 ..< s.zcs.len:
          if s.zcs[i] == zc:
            idx = i
            break
        if idx >= 0:
          s.zcs.delete(idx)
          if idx < s.ws.len: s.ws.delete(idx)
    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if s.onMessage != nil:
        s.onMessage(data)
    zc.onClose = proc(zc: ZmtpConnection) =
      var idx = -1
      for i in 0 ..< s.zcs.len:
        if s.zcs[i] == zc:
          idx = i
          break
      if idx >= 0:
        s.zcs.delete(idx)
        if idx < s.ws.len: s.ws.delete(idx)
      if s.zcs.len == 0 and s.onClose != nil:
        s.onClose()
    wsSendGreeting(zc, ws)
  s.wss.listen(address, port)

proc connect*(s: WsPullSocket; address: string; port: int; path: string = "/") =
  let self = s
  var closed = false
  discard connectWs(s.loop, address, port, path,
    onOpen = proc(ws: WsConnection) {.closure.} =
      let zc = initZmtpConnection(ws.conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "PULL"
      self.ws.add(ws)
      self.zcs.add(zc)
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) {.closure.} =
        if zc.peerSocketType != "PUSH":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
      zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) {.closure.} =
        if self.onMessage != nil:
          self.onMessage(data)
      zc.onClose = proc(zc: ZmtpConnection) {.closure.} =
        if closed: return
        closed = true
        var idx = -1
        for i in 0 ..< self.zcs.len:
          if self.zcs[i] == zc:
            idx = i
            break
        if idx >= 0:
          self.zcs.delete(idx)
          if idx < self.ws.len: self.ws.delete(idx)
        if self.zcs.len == 0 and self.onClose != nil:
          self.onClose()
      wsSendGreeting(zc, ws),
    onClose = proc(ws: WsConnection; code: int; reason: string) {.closure.} =
      if closed: return
      closed = true
      if self.zcs.len == 0 and self.onClose != nil:
        self.onClose(),
    onError = proc(ws: WsConnection; err: string) {.closure.} =
      discard)
