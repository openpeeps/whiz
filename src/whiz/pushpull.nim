# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## PUSH/PULL — Pipeline socket pattern.

import std/[sequtils]
import powpow/[loop, types, net/tcp]
import ./zmtp
import ./auth
import ./curve

export loop, types, tcp, zmtp, auth, curve

type
  PushSocket* = ref object
    loop:       Loop
    server:     TcpServer
    conns:      seq[ZmtpConnection]
    rrIndex:    int
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]

  PullSocket* = ref object
    loop:       Loop
    server:     TcpServer
    conns:      seq[ZmtpConnection]
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]

proc setCurveKeypair*(pull: PullSocket; publicKey, secretKey: array[32, uint8]) =
  pull.authMech = "CURVE"
  pull.authPubKey = publicKey
  pull.authSecKey = secretKey

proc setCurveClient*(push: PushSocket; publicKey, secretKey, serverKey: array[32, uint8]) =
  push.authMech = "CURVE"
  push.authPubKey = publicKey
  push.authSecKey = secretKey
  push.authSrvKey = serverKey

# ── PushSocket ────────────────────────────────────────────────────────────────

proc newPushSocket*(loop: Loop): PushSocket =
  PushSocket(loop: loop, conns: @[], rrIndex: 0)

proc send*(push: PushSocket; data: string): bool {.discardable.} =
  ## Round-robins to the next connected PULL. Returns false when nothing was
  ## sent (no workers, or the selected worker's transport is dead).
  if push.conns.len == 0: return false
  if push.rrIndex >= push.conns.len: push.rrIndex = 0
  let idx = push.rrIndex
  push.rrIndex = (push.rrIndex + 1) mod push.conns.len
  let zc = push.conns[idx]
  if zc.state == ZmtpEstablished:
    return zc.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc close*(push: PushSocket) =
  for zc in push.conns:
    zc.close()
  push.conns.setLen(0)
  if push.server != nil:
    push.server.close()
    push.server = nil
  if push.onClose != nil: push.onClose()

proc `bind`*(push: PushSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = push

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "PUSH"
    conn.data = cast[pointer](zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
    ps.conns.add(zc)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PULL":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        ps.conns.keepItIf(it != zc)

    zc.onClose = proc(zc: ZmtpConnection) =
      ps.conns.keepItIf(it != zc)
      if ps.conns.len == 0 and ps.onClose != nil:
        ps.onClose()

  proc onConnClosed(conn: Connection) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil and zc.state != ZmtpClosed:
      zc.state = ZmtpClosed
      if zc.onClose != nil: zc.onClose(zc)

  ps.server = newTcpServer(ps.loop, onAccept = onAccept, onData = feedData,
                           onClose = onConnClosed)
  case transport
  of TransportTcp:
    ps.server.listen(address, port)
  of TransportUnix:
    when not defined(windows):
      ps.server.listenUnix(address)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

proc connect*(push: PushSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  let ps = push

  proc onConnect(conn: Connection) =
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "PUSH"
    conn.data = cast[pointer](zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
      if ps.authSrvKey != default(array[32, uint8]):
        curve.setCurveServerKey(zc, ps.authSrvKey)
    ps.conns.add(zc)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PULL":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        ps.conns.keepItIf(it != zc)

    zc.onClose = proc(zc: ZmtpConnection) =
      ps.conns.keepItIf(it != zc)
      if ps.conns.len == 0 and ps.onClose != nil:
        ps.onClose()

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
    push.loop.connect(address, port, onConnect, feedData, onClose)
  of TransportUnix:
    when not defined(windows):
      push.loop.connectUnix(address, onConnect, feedData, onClose)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

# ── PullSocket ────────────────────────────────────────────────────────────────

proc newPullSocket*(loop: Loop): PullSocket =
  PullSocket(loop: loop, conns: @[])

proc send*(pull: PullSocket; data: string): bool {.discardable.} =
  ## Sends to the most recently connected PUSH peer. Returns false when
  ## nothing was sent (no peers, or that peer's transport is dead).
  if pull.conns.len == 0: return false
  let zc = pull.conns[^1]
  if zc.state == ZmtpEstablished:
    return zc.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc close*(pull: PullSocket) =
  for zc in pull.conns:
    zc.close()
  pull.conns.setLen(0)
  if pull.server != nil:
    pull.server.close()
    pull.server = nil
  if pull.onClose != nil: pull.onClose()

proc `bind`*(pull: PullSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = pull

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "PULL"
    conn.data = cast[pointer](zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PUSH":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return

    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if ps.onMessage != nil:
        ps.onMessage(data)

    zc.onClose = proc(zc: ZmtpConnection) =
      ps.conns.keepItIf(it != zc)
      if ps.conns.len == 0 and ps.onClose != nil:
        ps.onClose()

    ps.conns.add(zc)

  proc onConnClosed(conn: Connection) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil and zc.state != ZmtpClosed:
      zc.state = ZmtpClosed
      if zc.onClose != nil: zc.onClose(zc)

  ps.server = newTcpServer(ps.loop, onAccept = onAccept, onData = feedData,
                           onClose = onConnClosed)
  case transport
  of TransportTcp:
    ps.server.listen(address, port)
  of TransportUnix:
    when not defined(windows):
      ps.server.listenUnix(address)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

proc connect*(pull: PullSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  let ps = pull

  proc onConnect(conn: Connection) =
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "PULL"
    conn.data = cast[pointer](zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
      if ps.authSrvKey != default(array[32, uint8]):
        curve.setCurveServerKey(zc, ps.authSrvKey)
    ps.conns.add(zc)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PUSH":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        ps.conns.keepItIf(it != zc)

    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if ps.onMessage != nil:
        ps.onMessage(data)

    zc.onClose = proc(zc: ZmtpConnection) =
      ps.conns.keepItIf(it != zc)
      if ps.conns.len == 0 and ps.onClose != nil:
        ps.onClose()

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
    pull.loop.connect(address, port, onConnect, feedData, onClose)
  of TransportUnix:
    when not defined(windows):
      pull.loop.connectUnix(address, onConnect, feedData, onClose)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")
