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
import ./tls

export loop, types, tcp, zmtp, auth, curve, tls

type
  PairSocket* = ref object
    loop:       Loop
    ownsLoop: bool
    batchWrites: bool
    server:     TcpServer
    conn*:       ZmtpConnection
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    authUser, authPass: string
    authZapHandler: ZapHandler
    tls: TlsConfig
    authVouchPub: array[32, uint8]
    authVouchSec: array[64, uint8]
    hasVouch: bool
    expectedVouchPub: array[32, uint8]
    requireVouch: bool

proc setCurveKeypair*(pair: PairSocket; publicKey, secretKey: array[32, uint8]) =
  pair.authMech = "CURVE"
  pair.authPubKey = publicKey
  pair.authSecKey = secretKey

proc setCurveClient*(pair: PairSocket; publicKey, secretKey, serverKey: array[32, uint8]) =
  pair.authMech = "CURVE"
  pair.authPubKey = publicKey
  pair.authSecKey = secretKey
  pair.authSrvKey = serverKey

proc setCurveVouch*(pair: PairSocket; vouchPub: array[32, uint8];
                    vouchSec: array[64, uint8]) =
  ## Enables CURVE vouch (Ed25519 key continuity) on this socket.
  ## Clients sign HELLO eph key; servers verify when requireVouch is set.
  pair.authVouchPub = vouchPub
  pair.authVouchSec = vouchSec
  pair.hasVouch = true
  if pair.authMech == "": pair.authMech = "CURVE"

proc setCurveExpectedVouch*(pair: PairSocket; vouchPub: array[32, uint8]) =
  ## Requires peer HELLO to carry a valid vouch for this Ed25519 key.
  pair.expectedVouchPub = vouchPub
  pair.requireVouch = true
  if pair.authMech == "": pair.authMech = "CURVE"

proc setPlainAuth*(pair: PairSocket; username, password: string) =
  pair.authMech = "PLAIN"
  pair.authUser = username
  pair.authPass = password

proc onAuthenticate*(pair: PairSocket; handler: ZapHandler) =
  pair.authZapHandler = handler

proc setTlsServer*(pair: PairSocket; certFile, keyFile: string) =
  ## Enables TLS for accepted connections (TCP only, POSIX only). Call
  ## before bind. Raises SslError when the cert and key do not match.
  enableTlsServer(pair.tls, certFile, keyFile)

proc setTlsClient*(pair: PairSocket; verifyPeer = true; serverName = "") =
  ## Enables TLS for outbound connections (TCP only, POSIX only). Call
  ## before connect. Pass verifyPeer=false for self-signed test certs.
  enableTlsClient(pair.tls, verifyPeer, serverName)

proc setTlsAlpn*(pair: PairSocket; protos: openArray[string]) =
  ## Advertises ALPN protocols (e.g. `["zmtp/3.0"]`). Call after
  ## setTlsServer/setTlsClient, before bind/connect.
  if pair.tls != nil:
    pair.tls.setTlsAlpn(protos)

proc setTlsKtlsAuto*(pair: PairSocket; zerocopySendfile = false) =
  ## Best-effort kernel-TLS offload for subsequently wrapped connections.
  ## Linux + OpenSSL enable-ktls only; safe no-op elsewhere.
  if pair.tls == nil: pair.tls = TlsConfig()
  pair.tls.enableTlsKtls(zerocopySendfile)

proc setTlsKtlsRequired*(pair: PairSocket; zerocopySendfile = false) =
  ## Like setTlsKtlsAuto but fails closed when offload cannot engage.
  if pair.tls == nil: pair.tls = TlsConfig()
  pair.tls.configureTlsKtls(KtlsRequired, zerocopySendfile)

proc setBatching*(pair: PairSocket; on: bool = true) =
  ## Coalesces sends into bulk writes (see `setBatching` in powpow).
  ## Enable for floods; leave off for lockstep latency paths.
  pair.batchWrites = on
  if pair.conn != nil:
    pair.conn.conn.setBatching(on)

proc pendingBytes*(pair: PairSocket): int =
  ## Bytes queued in powpow write buffers for this connection (0 = drained).
  if pair.conn != nil: pair.conn.pendingBytes()
  else: 0


proc applySocketAuth(zc: ZmtpConnection; pair: PairSocket) =
  if pair.authMech == "PLAIN":
    auth.setPlainAuth(zc, pair.authUser, pair.authPass)
    if pair.authZapHandler != nil:
      auth.onAuthenticate(zc, pair.authZapHandler)
  elif pair.authMech == "CURVE":
    curve.setCurveKeypair(zc, pair.authPubKey, pair.authSecKey)
    if pair.authSrvKey != default(array[32, uint8]):
      curve.setCurveServerKey(zc, pair.authSrvKey)
    if pair.hasVouch:
      curve.setCurveVouch(zc, pair.authVouchPub, pair.authVouchSec)
    if pair.requireVouch:
      curve.setCurveExpectedVouch(zc, pair.expectedVouchPub)

proc newPairSocket*(loop: Loop): PairSocket =
  PairSocket(loop: loop, ownsLoop: false)

proc newPairSocket*(): PairSocket =
  ## Creates a PAIR socket with its own event loop (no manual newLoop).
  ## Use getLoop/run/stop; close() frees the owned loop.
  PairSocket(loop: newLoop(), ownsLoop: true)

proc getLoop*(s: PairSocket): Loop {.inline.} = s.loop
proc run*(s: PairSocket) {.inline.} = s.loop.run()
proc stop*(s: PairSocket) {.inline.} = s.loop.stop()

proc send*(pair: PairSocket; data: string): bool {.discardable.} =
  ## Returns false when the message was not delivered (no connection,
  ## handshake incomplete, or the underlying transport is dead).
  if pair.conn != nil:
    if pair.conn.state == ZmtpEstablished:
      return pair.conn.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc close*(pair: PairSocket) =
  if pair.conn != nil:
    pair.conn.close()
    if pair.onClose != nil: pair.onClose()
    pair.conn = nil
  if pair.server != nil:
    pair.server.close()
    pair.server = nil
  if pair.ownsLoop and pair.loop != nil:
    pair.loop.close()

proc `bind`*(pair: PairSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = pair

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    if transport == TransportTcp and not wrapServerConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "PAIR"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
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

  proc onConnClosed(conn: Connection) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil and zc.state != ZmtpClosed:
      zc.state = ZmtpClosed
      if zc.onClose != nil: zc.onClose(zc)

  ps.server = newTcpServer(pair.loop, onAccept = onAccept, onData = feedData,
                           onClose = onConnClosed)
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
    if transport == TransportTcp and not wrapClientConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "PAIR"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
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
