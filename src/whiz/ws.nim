# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## WebSocket transport for ZMTP 3.0. Maps each ZMTP frame to a single WebSocket
## binary message. Powered by powpow's RFC 6455 WebSocket implementation.

import std/[random, base64, strutils, sequtils]
import powpow/[loop, types, net/tcp, proto/ws]
import ./zmtp
export loop, types, tcp, ws, zmtp

randomize()

# ── WS client handshake ───────────────────────────────────────────────────────

proc generateWsKey(): string =
  var keyBytes: array[16, byte]
  for i in 0..15:
    keyBytes[i] = byte(rand(255))
  result = encode(keyBytes)

proc buildWsUpgradeRequest(host: string, port: int, path: string, key: string): string =
  result = "GET " & path & " HTTP/1.1\r\n" &
           "Host: " & host & ":" & $port & "\r\n" &
           "Upgrade: websocket\r\n" &
           "Connection: Upgrade\r\n" &
           "Sec-WebSocket-Key: " & key & "\r\n" &
           "Sec-WebSocket-Version: 13\r\n" &
           "\r\n"

type
  WsUpgradeResult* = object
    success: bool
    remaining: seq[byte]
    error: string

proc parseWsUpgradeResponse(data: openArray[byte], key: string): WsUpgradeResult =
  var s = newString(data.len)
  if data.len > 0:
    copyMem(addr s[0], unsafeAddr data[0], data.len)
  let headerEnd = s.find("\r\n\r\n")
  if headerEnd == -1:
    return WsUpgradeResult(success: false, error: "Incomplete HTTP response")
  if "101" notin s:
    return WsUpgradeResult(success: false,
      error: "Unexpected status: " & s.splitLines()[0])
  let expectedAccept = computeAcceptKey(key)
  if expectedAccept notin s:
    return WsUpgradeResult(success: false, error: "Invalid accept key")
  result.success = true
  if data.len > headerEnd + 4:
    result.remaining = @data[headerEnd + 4 .. ^1]

# ── ZMTP ↔ WebSocket bridge ──────────────────────────────────────────────────

proc wsBridge(zc: ZmtpConnection, ws: WsConnection) =
  zc.sendOverride = proc(zc: ZmtpConnection; data: openArray[byte]): int =
    ws.sendBinary(data)
    return data.len
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

proc send*(s: WsPairSocket; data: string) =
  if s.zc != nil and s.zc.state == ZmtpEstablished:
    s.zc.sendMessage(data.toOpenArrayByte(0, data.high))

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
  var key = generateWsKey()
  var upgradeBuf: seq[byte]
  var upgraded = false
  proc onConnect(conn: Connection) =
    let req = buildWsUpgradeRequest(address, port, path, key)
    discard conn.send(req)
    s.conn = conn
  proc onData(conn: Connection; data: openArray[byte]) =
    if not upgraded:
      upgradeBuf.add(data)
      let bufStr = cast[string](upgradeBuf)
      if "\r\n\r\n" notin bufStr: return
      let res = parseWsUpgradeResponse(upgradeBuf, key)
      if not res.success:
        if s.zc != nil and s.zc.onError != nil:
          s.zc.onError(s.zc, res.error)
        return
      upgraded = true
      let ws = newWsConnection(conn)
      s.ws = ws
      let zc = initZmtpConnection(conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "PAIR"
      conn.data = cast[pointer](zc)
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
      upgradeBuf.setLen(0)
      wsSendGreeting(zc, ws)
      if res.remaining.len > 0:
        ws.parseWsFrames(res.remaining)
    else:
      if s.ws != nil:
        s.ws.parseWsFrames(data)
  proc onCloseConn(conn: Connection) =
    if s.zc != nil and s.zc.state != ZmtpClosed:
      s.zc.state = ZmtpClosed
      if s.zc.onClose != nil:
        s.zc.onClose(s.zc)
    s.zc = nil
    s.ws = nil
    if s.onClose != nil: s.onClose()
  s.loop.connect(address, port, onConnect, onData, onCloseConn)

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

proc publish*(s: WsPubSocket; data: string) =
  for sub in s.subs:
    if sub.topics.len > 0:
      for t in sub.topics:
        if t.len == 0 or data.startsWith(t):
          sub.zc.sendMessage(data.toOpenArrayByte(0, data.high))
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
    conns: seq[Connection]
    subscriptions: seq[string]
    onMessage*: proc(topic: openArray[byte]; data: openArray[byte]) {.closure.}
    onClose*: proc() {.closure.}

proc newWsSubSocket*(loop: Loop): WsSubSocket =
  WsSubSocket(loop: loop, ws: @[], zcs: @[], conns: @[], subscriptions: @[])

proc close*(s: WsSubSocket) =
  for zc in s.zcs:
    zc.close()
  s.zcs.setLen(0)
  s.ws.setLen(0)
  s.conns.setLen(0)

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
  var key = generateWsKey()
  var upgradeBuf: seq[byte]
  var upgraded = false
  var closed = false
  proc onConnect(conn: Connection) =
    let req = buildWsUpgradeRequest(address, port, path, key)
    discard conn.send(req)
    s.conns.add(conn)
  proc onData(conn: Connection; data: openArray[byte]) =
    if not upgraded:
      upgradeBuf.add(data)
      let bufStr = cast[string](upgradeBuf)
      if "\r\n\r\n" notin bufStr: return
      let res = parseWsUpgradeResponse(upgradeBuf, key)
      if not res.success:
        if s.zcs.len > 0 and s.zcs[^1].onError != nil:
          s.zcs[^1].onError(s.zcs[^1], res.error)
        return
      upgraded = true
      let ws = newWsConnection(conn)
      s.ws.add(ws)
      let zc = initZmtpConnection(conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "SUB"
      conn.data = cast[pointer](zc)
      s.zcs.add(zc)
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) =
        if zc.peerSocketType != "PUB":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
          return
        for topic in s.subscriptions:
          var frame = newSeq[byte](1 + topic.len)
          frame[0] = 1
          if topic.len > 0:
            copyMem(addr frame[1], addr topic[0], topic.len)
          discard zc.sendCommand("SUBSCRIBE", frame)
      zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        if s.onMessage != nil:
          s.onMessage(data, data)
      zc.onClose = proc(zc: ZmtpConnection) =
        if closed: return
        closed = true
        var idx = -1
        for i in 0 ..< s.zcs.len:
          if s.zcs[i] == zc:
            idx = i
            break
        if idx >= 0:
          s.zcs.delete(idx)
          if idx < s.ws.len: s.ws.delete(idx)
          if idx < s.conns.len: s.conns.delete(idx)
        if s.onClose != nil: s.onClose()
      upgradeBuf.setLen(0)
      wsSendGreeting(zc, ws)
      if res.remaining.len > 0:
        ws.parseWsFrames(res.remaining)
    else:
      if s.ws.len > 0:
        s.ws[^1].parseWsFrames(data)
  proc onCloseConn(conn: Connection) =
    if closed: return
    closed = true
    var idx = -1
    for i in 0 ..< s.conns.len:
      if s.conns[i] == conn:
        idx = i
        break
    if idx >= 0:
      if idx < s.ws.len: s.ws.delete(idx)
      s.conns.delete(idx)
    if s.onClose != nil: s.onClose()
  s.loop.connect(address, port, onConnect, onData, onCloseConn)

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

proc send*(s: WsReqSocket; data: string) =
  if s.zc != nil and s.zc.state == ZmtpEstablished and not s.waiting:
    s.waiting = true
    s.zc.sendMessage(data.toOpenArrayByte(0, data.high))

proc close*(s: WsReqSocket) =
  if s.zc != nil:
    s.zc.close()
    s.zc = nil
  if s.onClose != nil: s.onClose()

proc connect*(s: WsReqSocket; address: string; port: int; path: string = "/") =
  var key = generateWsKey()
  var upgradeBuf: seq[byte]
  var upgraded = false
  proc onConnect(conn: Connection) =
    let req = buildWsUpgradeRequest(address, port, path, key)
    discard conn.send(req)
    s.conn = conn
  proc onData(conn: Connection; data: openArray[byte]) =
    if not upgraded:
      upgradeBuf.add(data)
      let bufStr = cast[string](upgradeBuf)
      if "\r\n\r\n" notin bufStr: return
      let res = parseWsUpgradeResponse(upgradeBuf, key)
      if not res.success:
        if s.zc != nil and s.zc.onError != nil:
          s.zc.onError(s.zc, res.error)
        return
      upgraded = true
      let ws = newWsConnection(conn)
      s.ws = ws
      let zc = initZmtpConnection(conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "REQ"
      conn.data = cast[pointer](zc)
      s.zc = zc
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) =
        if zc.peerSocketType != "REP":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
      zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        s.waiting = false
        if s.onReply != nil:
          s.onReply(data)
      zc.onClose = proc(zc: ZmtpConnection) =
        s.zc = nil
        s.ws = nil
        if s.onClose != nil: s.onClose()
      upgradeBuf.setLen(0)
      wsSendGreeting(zc, ws)
      if res.remaining.len > 0:
        ws.parseWsFrames(res.remaining)
    else:
      if s.ws != nil:
        s.ws.parseWsFrames(data)
  proc onCloseConn(conn: Connection) =
    if s.zc != nil:
      if s.zc.state != ZmtpClosed:
        s.zc.state = ZmtpClosed
        if s.zc.onClose != nil:
          s.zc.onClose(s.zc)
    s.zc = nil
    s.ws = nil
    if s.onClose != nil: s.onClose()
  s.loop.connect(address, port, onConnect, onData, onCloseConn)

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

proc send*(s: WsRepSocket; data: string) =
  if s.zc != nil and s.zc.state == ZmtpEstablished and s.hasRequest:
    s.hasRequest = false
    s.zc.sendMessage(data.toOpenArrayByte(0, data.high))

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

proc send*(s: WsPushSocket; data: string) =
  if s.zcs.len == 0: return
  if s.rrIndex >= s.zcs.len: s.rrIndex = 0
  let idx = s.rrIndex
  s.rrIndex = (s.rrIndex + 1) mod s.zcs.len
  let zc = s.zcs[idx]
  if zc.state == ZmtpEstablished:
    zc.sendMessage(data.toOpenArrayByte(0, data.high))

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
  var key = generateWsKey()
  var upgradeBuf: seq[byte]
  var upgraded = false
  var closed = false
  proc onConnect(conn: Connection) =
    let req = buildWsUpgradeRequest(address, port, path, key)
    discard conn.send(req)
  proc onData(conn: Connection; data: openArray[byte]) =
    if not upgraded:
      upgradeBuf.add(data)
      let bufStr = cast[string](upgradeBuf)
      if "\r\n\r\n" notin bufStr: return
      let res = parseWsUpgradeResponse(upgradeBuf, key)
      if not res.success:
        if s.zcs.len > 0 and s.zcs[^1].onError != nil:
          s.zcs[^1].onError(s.zcs[^1], res.error)
        return
      upgraded = true
      let ws = newWsConnection(conn)
      s.ws.add(ws)
      let zc = initZmtpConnection(conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "PUSH"
      conn.data = cast[pointer](zc)
      s.zcs.add(zc)
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) =
        if zc.peerSocketType != "PULL":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
      zc.onClose = proc(zc: ZmtpConnection) =
        if closed: return
        closed = true
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
      upgradeBuf.setLen(0)
      wsSendGreeting(zc, ws)
      if res.remaining.len > 0:
        ws.parseWsFrames(res.remaining)
    else:
      if s.ws.len > 0:
        s.ws[^1].parseWsFrames(data)
  proc onCloseConn(conn: Connection) =
    if closed: return
    closed = true
    if s.zcs.len == 0 and s.onClose != nil:
      s.onClose()
  s.loop.connect(address, port, onConnect, onData, onCloseConn)

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

proc send*(s: WsPullSocket; data: string) =
  if s.zcs.len == 0: return
  let zc = s.zcs[^1]
  if zc.state == ZmtpEstablished:
    zc.sendMessage(data.toOpenArrayByte(0, data.high))

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
  var key = generateWsKey()
  var upgradeBuf: seq[byte]
  var upgraded = false
  var closed = false
  proc onConnect(conn: Connection) =
    let req = buildWsUpgradeRequest(address, port, path, key)
    discard conn.send(req)
  proc onData(conn: Connection; data: openArray[byte]) =
    if not upgraded:
      upgradeBuf.add(data)
      let bufStr = cast[string](upgradeBuf)
      if "\r\n\r\n" notin bufStr: return
      let res = parseWsUpgradeResponse(upgradeBuf, key)
      if not res.success:
        if s.zcs.len > 0 and s.zcs[^1].onError != nil:
          s.zcs[^1].onError(s.zcs[^1], res.error)
        return
      upgraded = true
      let ws = newWsConnection(conn)
      s.ws.add(ws)
      let zc = initZmtpConnection(conn, asServer = false, "NULL", skipGreeting = true)
      zc.socketType = "PULL"
      conn.data = cast[pointer](zc)
      s.zcs.add(zc)
      wsBridge(zc, ws)
      zc.onReady = proc(zc: ZmtpConnection) =
        if zc.peerSocketType != "PUSH":
          discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
          zc.close()
      zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        if s.onMessage != nil:
          s.onMessage(data)
      zc.onClose = proc(zc: ZmtpConnection) =
        if closed: return
        closed = true
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
      upgradeBuf.setLen(0)
      wsSendGreeting(zc, ws)
      if res.remaining.len > 0:
        ws.parseWsFrames(res.remaining)
    else:
      if s.ws.len > 0:
        s.ws[^1].parseWsFrames(data)
  proc onCloseConn(conn: Connection) =
    if closed: return
    closed = true
    if s.zcs.len == 0 and s.onClose != nil:
      s.onClose()
  s.loop.connect(address, port, onConnect, onData, onCloseConn)
