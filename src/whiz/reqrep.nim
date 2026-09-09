# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## REQ/REP — Request-Reply socket pattern.

import powpow/[loop, types, net/tcp]
import ./zmtp
import ./auth
import ./curve
import ./tls

export loop, types, tcp, zmtp, auth, curve, tls

type
  ReqSocket* = ref object
    loop:       Loop
    conn:       ZmtpConnection
    waiting:    bool
    onReply*:   proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    tls: TlsConfig

  RepSocket* = ref object
    loop:       Loop
    conn:       ZmtpConnection
    server:     TcpServer
    hasRequest: bool
    onRequest*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    tls: TlsConfig

proc setCurveKeypair*(rep: RepSocket; publicKey, secretKey: array[32, uint8]) =
  rep.authMech = "CURVE"
  rep.authPubKey = publicKey
  rep.authSecKey = secretKey

proc setCurveClient*(req: ReqSocket; publicKey, secretKey, serverKey: array[32, uint8]) =
  req.authMech = "CURVE"
  req.authPubKey = publicKey
  req.authSecKey = secretKey
  req.authSrvKey = serverKey

proc setTlsServer*(s: ReqSocket | RepSocket; certFile, keyFile: string) =
  ## Enables TLS for accepted connections (TCP only, POSIX only). Call
  ## before bind. Raises SslError when the cert and key do not match.
  enableTlsServer(s.tls, certFile, keyFile)

proc setTlsClient*(s: ReqSocket | RepSocket; verifyPeer = true; serverName = "") =
  ## Enables TLS for outbound connections (TCP only, POSIX only). Call
  ## before connect. Pass verifyPeer=false for self-signed test certs.
  enableTlsClient(s.tls, verifyPeer, serverName)

# ── REQ ─────────────────────────────────────────────────────────────────────

proc newReqSocket*(loop: Loop): ReqSocket =
  ReqSocket(loop: loop)

proc send*(req: ReqSocket; data: string): bool {.discardable.} =
  ## Returns false when no request was sent (no connection, not established,
  ## a reply is still pending, or the transport is dead).
  if req.conn != nil and req.conn.state == ZmtpEstablished and not req.waiting:
    req.waiting = true
    if req.conn.sendMessage(data.toOpenArrayByte(0, data.high)):
      return true
    # Transport died mid-send; release the alternation lock so the socket
    # does not wedge in `waiting` forever.
    req.waiting = false
  false

proc close*(req: ReqSocket) =
  if req.conn != nil:
    req.conn.close()
    req.conn = nil
  if req.onClose != nil: req.onClose()

proc connect*(req: ReqSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  let ps = req

  proc onConnect(conn: Connection) =
    if transport == TransportTcp and not wrapClientConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "REQ"
    conn.data = cast[pointer](zc)
    ps.conn = zc
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
      if ps.authSrvKey != default(array[32, uint8]):
        curve.setCurveServerKey(zc, ps.authSrvKey)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "REP":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return

    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      ps.waiting = false
      if ps.onReply != nil:
        ps.onReply(data)

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
    req.loop.connect(address, port, onConnect, feedData, onClose)
  of TransportUnix:
    when not defined(windows):
      req.loop.connectUnix(address, onConnect, feedData, onClose)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

# ── REP ─────────────────────────────────────────────────────────────────────

proc newRepSocket*(loop: Loop): RepSocket =
  RepSocket(loop: loop)

proc send*(rep: RepSocket; data: string): bool {.discardable.} =
  ## Returns false when no reply was sent (no connection, not established,
  ## no pending request, or the transport is dead).
  if rep.conn != nil and rep.conn.state == ZmtpEstablished and rep.hasRequest:
    rep.hasRequest = false
    if rep.conn.sendMessage(data.toOpenArrayByte(0, data.high)):
      return true
    # Transport died mid-send; allow a fresh request to be accepted.
    rep.hasRequest = true
  false

proc close*(rep: RepSocket) =
  if rep.conn != nil:
    rep.conn.close()
    if rep.onClose != nil: rep.onClose()
    rep.conn = nil
  if rep.server != nil:
    rep.server.close()
    rep.server = nil

proc `bind`*(rep: RepSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = rep

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    if transport == TransportTcp and not wrapServerConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "REP"
    conn.data = cast[pointer](zc)
    ps.conn = zc
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "REQ":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return
      if ps.server != nil:
        ps.server.close()
        ps.server = nil

    zc.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
      if not ps.hasRequest:
        ps.hasRequest = true
        if ps.onRequest != nil:
          ps.onRequest(data)

    zc.onClose = proc(zc: ZmtpConnection) =
      ps.conn = nil
      if ps.onClose != nil: ps.onClose()

  proc onConnClosed(conn: Connection) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil and zc.state != ZmtpClosed:
      zc.state = ZmtpClosed
      ps.hasRequest = false
      if zc.onClose != nil: zc.onClose(zc)

  ps.server = newTcpServer(rep.loop, onAccept = onAccept, onData = feedData,
                           onClose = onConnClosed)
  case transport
  of TransportTcp:
    ps.server.listen(address, port)
  of TransportUnix:
    when not defined(windows):
      ps.server.listenUnix(address)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")
