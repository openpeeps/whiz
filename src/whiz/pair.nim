# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## Exclusive PAIR socket pattern — one-to-one bidirectional communication.

import powpow/[loop, types, net/tcp]
import ./zmtp
import ./auth
import ./curve

export loop, types, tcp, zmtp, auth, curve

type
  PairSocket* = ref object
    loop:       Loop
    server:     TcpServer
    conn*:       ZmtpConnection
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    authUser, authPass: string
    authZapHandler: ZapHandler

proc setCurveKeypair*(pair: PairSocket; publicKey, secretKey: array[32, uint8]) =
  pair.authMech = "CURVE"
  pair.authPubKey = publicKey
  pair.authSecKey = secretKey

proc setCurveClient*(pair: PairSocket; publicKey, secretKey, serverKey: array[32, uint8]) =
  pair.authMech = "CURVE"
  pair.authPubKey = publicKey
  pair.authSecKey = secretKey
  pair.authSrvKey = serverKey

proc setPlainAuth*(pair: PairSocket; username, password: string) =
  pair.authMech = "PLAIN"
  pair.authUser = username
  pair.authPass = password

proc onAuthenticate*(pair: PairSocket; handler: ZapHandler) =
  pair.authZapHandler = handler

proc applySocketAuth(zc: ZmtpConnection; pair: PairSocket) =
  if pair.authMech == "PLAIN":
    auth.setPlainAuth(zc, pair.authUser, pair.authPass)
    if pair.authZapHandler != nil:
      auth.onAuthenticate(zc, pair.authZapHandler)
  elif pair.authMech == "CURVE":
    curve.setCurveKeypair(zc, pair.authPubKey, pair.authSecKey)
    if pair.authSrvKey != default(array[32, uint8]):
      curve.setCurveServerKey(zc, pair.authSrvKey)

proc newPairSocket*(loop: Loop): PairSocket =
  PairSocket(loop: loop)

proc send*(pair: PairSocket; data: string) =
  if pair.conn != nil:
    if pair.conn.state == ZmtpEstablished:
      pair.conn.sendMessage(data.toOpenArrayByte(0, data.high))

proc close*(pair: PairSocket) =
  if pair.conn != nil:
    pair.conn.close()
    if pair.onClose != nil: pair.onClose()
    pair.conn = nil
  if pair.server != nil:
    pair.server.close()
    pair.server = nil

proc `bind`*(pair: PairSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = pair

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "PAIR"
    conn.data = cast[pointer](zc)
    ps.conn = zc
    applySocketAuth(zc, ps)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PAIR":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return

    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if ps.onMessage != nil:
        ps.onMessage(data)

    zc.onClose = proc(zc: ZmtpConnection) =
      ps.conn = nil
      if ps.onClose != nil: ps.onClose()

  ps.server = newTcpServer(pair.loop, onAccept = onAccept, onData = feedData)
  case transport
  of TransportTcp:
    ps.server.listen(address, port)
  of TransportUnix:
    when not defined(windows):
      ps.server.listenUnix(address)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

proc connect*(pair: PairSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  let ps = pair

  proc setupConn(conn: Connection) =
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "PAIR"
    conn.data = cast[pointer](zc)
    ps.conn = zc
    applySocketAuth(zc, ps)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PAIR":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return

    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if ps.onMessage != nil:
        ps.onMessage(data)

    zc.onClose = proc(zc: ZmtpConnection) =
      ps.conn = nil
      if ps.onClose != nil: ps.onClose()

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
    pair.loop.connect(address, port, setupConn, feedData, onClose)
  of TransportUnix:
    when not defined(windows):
      pair.loop.connectUnix(address, setupConn, feedData, onClose)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")
