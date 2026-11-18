# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## CLIENT/SERVER — Stream socket pattern.
##
## CLIENT talks to one SERVER with no send/receive lockstep. SERVER talks
## to many CLIENTs and routes each reply through the ServerClient handle
## the request arrived on. The handle id is the routing id, matching the
## [routing-id, payload...] envelope shape of the ZMTP CLIENT/SERVER spec.

import std/[sequtils]
import powpow/[loop, types, net/tcp]
import ./zmtp
import ./auth
import ./curve
import ./tls

export loop, types, tcp, zmtp, auth, curve, tls

type
  ServerClient* = ref object
    id*: uint32
    zc*: ZmtpConnection

  ClientSocket* = ref object
    loop:       Loop
    ownsLoop: bool
    batchWrites: bool
    server:     TcpServer
    conn:       ZmtpConnection
    onReply*:   proc(data: openArray[byte]) {.closure.}
    onReplyMultipart*: proc(frames: seq[seq[byte]]) {.closure.}
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

  ServerSocket* = ref object
    loop:       Loop
    ownsLoop: bool
    batchWrites: bool
    server:     TcpServer
    clients:    seq[ServerClient]
    nextId:     uint32
    onRequest*: proc(client: ServerClient; data: openArray[byte]) {.closure.}
    onRequestMultipart*: proc(client: ServerClient; frames: seq[seq[byte]]) {.closure.}
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

proc setCurveKeypair*(s: ClientSocket | ServerSocket; publicKey, secretKey: array[32, uint8]) =
  s.authMech = "CURVE"
  s.authPubKey = publicKey
  s.authSecKey = secretKey

proc setCurveClient*(s: ClientSocket | ServerSocket; publicKey, secretKey, serverKey: array[32, uint8]) =
  s.authMech = "CURVE"
  s.authPubKey = publicKey
  s.authSecKey = secretKey
  s.authSrvKey = serverKey

proc setPlainAuth*(s: ClientSocket | ServerSocket; username, password: string) =
  s.authMech = "PLAIN"
  s.authUser = username
  s.authPass = password

proc onAuthenticate*(s: ClientSocket | ServerSocket; handler: ZapHandler) =
  s.authZapHandler = handler

proc setTlsServer*(s: ClientSocket | ServerSocket; certFile, keyFile: string) =
  ## Enables TLS for accepted connections (TCP only, POSIX only). Call
  ## before bind. Raises SslError when the cert and key do not match.
  enableTlsServer(s.tls, certFile, keyFile)

proc setTlsClient*(s: ClientSocket | ServerSocket; verifyPeer = true; serverName = "") =
  ## Enables TLS for outbound connections (TCP only, POSIX only). Call
  ## before connect. Pass verifyPeer=false for self-signed test certs.
  enableTlsClient(s.tls, verifyPeer, serverName)

proc setTlsAlpn*(s: ClientSocket | ServerSocket; protos: openArray[string]) =
  if s.tls != nil:
    s.tls.setTlsAlpn(protos)

proc setTlsKtlsAuto*(s: ClientSocket | ServerSocket; zerocopySendfile = false) =
  if s.tls == nil: s.tls = TlsConfig()
  s.tls.enableTlsKtls(zerocopySendfile)

proc setTlsKtlsRequired*(s: ClientSocket | ServerSocket; zerocopySendfile = false) =
  if s.tls == nil: s.tls = TlsConfig()
  s.tls.configureTlsKtls(KtlsRequired, zerocopySendfile)

proc setCurveVouch*(s: ClientSocket | ServerSocket; vouchPub: array[32, uint8];
                     vouchSec: array[64, uint8]) =
  s.authVouchPub = vouchPub
  s.authVouchSec = vouchSec
  s.hasVouch = true
  if s.authMech == "": s.authMech = "CURVE"

proc setCurveExpectedVouch*(s: ClientSocket | ServerSocket; vouchPub: array[32, uint8]) =
  s.expectedVouchPub = vouchPub
  s.requireVouch = true
  if s.authMech == "": s.authMech = "CURVE"

proc setBatching*(cli: ClientSocket; on: bool = true) =
  ## Coalesces sends into bulk writes. Enable for floods.
  cli.batchWrites = on
  if cli.conn != nil:
    cli.conn.conn.setBatching(on)

proc setBatching*(srv: ServerSocket; on: bool = true) =
  ## Coalesces sends into bulk writes. Enable for floods.
  srv.batchWrites = on
  for c in srv.clients:
    if c.zc != nil:
      c.zc.conn.setBatching(on)

proc pendingBytes*(cli: ClientSocket): int =
  if cli.conn != nil: cli.conn.pendingBytes()
  else: 0

proc pendingBytes*(srv: ServerSocket): int =
  var total = 0
  for c in srv.clients:
    if c.zc != nil: total += c.zc.pendingBytes()
  total


proc applyClientServerAuth(zc: ZmtpConnection; authMech: string;
    pubKey, secKey, srvKey: array[32, uint8];
    user, pass: string; zap: ZapHandler;
    hasVouch = false; vouchPub: array[32, uint8] = default(array[32, uint8]);
    vouchSec: array[64, uint8] = default(array[64, uint8]);
    requireVouch = false; expectedVouch: array[32, uint8] = default(array[32, uint8])) =
  if authMech == "PLAIN":
    auth.setPlainAuth(zc, user, pass)
    if zap != nil:
      auth.onAuthenticate(zc, zap)
  elif authMech == "CURVE":
    curve.setCurveKeypair(zc, pubKey, secKey)
    if srvKey != default(array[32, uint8]):
      curve.setCurveServerKey(zc, srvKey)
    if hasVouch:
      curve.setCurveVouch(zc, vouchPub, vouchSec)
    if requireVouch:
      curve.setCurveExpectedVouch(zc, expectedVouch)

# ── ClientSocket ────────────────────────────────────────────────────────────

proc newClientSocket*(loop: Loop): ClientSocket =
  ClientSocket(loop: loop, ownsLoop: false)

proc newClientSocket*(): ClientSocket =
  ## Creates a CLIENT socket with its own event loop.
  ClientSocket(loop: newLoop(), ownsLoop: true)

proc getLoop*(s: ClientSocket): Loop {.inline.} = s.loop
proc run*(s: ClientSocket) {.inline.} = s.loop.run()
proc stop*(s: ClientSocket) {.inline.} = s.loop.stop()

proc send*(cli: ClientSocket; data: string): bool {.discardable.} =
  ## Sends one single-frame message to the SERVER. Returns false when the
  ## message was not delivered (no connection, handshake incomplete, or
  ## the underlying transport is dead).
  if cli.conn != nil and cli.conn.state == ZmtpEstablished:
    return cli.conn.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc sendMultipart*(cli: ClientSocket; frames: openArray[string]): bool {.discardable.} =
  ## Sends one multipart message to the SERVER. Returns false unless every
  ## frame was accepted by the transport.
  if cli.conn == nil or cli.conn.state != ZmtpEstablished: return false
  var fb = newSeq[seq[byte]](frames.len)
  for i, f in frames:
    fb[i] = newSeq[byte](f.len)
    if f.len > 0:
      copyMem(addr fb[i][0], unsafeAddr f[0], f.len)
  cli.conn.sendMultipart(fb)

proc close*(cli: ClientSocket) =
  if cli.conn != nil:
    cli.conn.close()
    cli.conn = nil
  if cli.server != nil:
    cli.server.close()
    cli.server = nil
  if cli.onClose != nil: cli.onClose()
  if cli.ownsLoop and cli.loop != nil:
    cli.loop.close()

proc wireClientHandlers(cli: ClientSocket; zc: ZmtpConnection) =
  zc.onReady = proc(zc: ZmtpConnection) =
    if zc.peerSocketType != "SERVER":
      discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
      zc.close()
  zc.onMultipart = proc(zc: ZmtpConnection; frames: seq[seq[byte]]) =
    if frames.len == 1 and cli.onReply != nil:
      cli.onReply(frames[0])
    if cli.onReplyMultipart != nil:
      cli.onReplyMultipart(frames)

proc `bind`*(cli: ClientSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = cli

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    if transport == TransportTcp and not wrapServerConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "CLIENT"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    applyClientServerAuth(zc, ps.authMech, ps.authPubKey, ps.authSecKey,
      ps.authSrvKey, ps.authUser, ps.authPass, ps.authZapHandler,
      ps.hasVouch, ps.authVouchPub, ps.authVouchSec, ps.requireVouch, ps.expectedVouchPub)
    ps.conn = zc
    wireClientHandlers(ps, zc)

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

proc connect*(cli: ClientSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  let ps = cli

  proc onConnect(conn: Connection) =
    if transport == TransportTcp and not wrapClientConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "CLIENT"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    applyClientServerAuth(zc, ps.authMech, ps.authPubKey, ps.authSecKey,
      ps.authSrvKey, ps.authUser, ps.authPass, ps.authZapHandler,
      ps.hasVouch, ps.authVouchPub, ps.authVouchSec, ps.requireVouch, ps.expectedVouchPub)
    ps.conn = zc
    wireClientHandlers(ps, zc)

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
    cli.loop.connect(address, port, onConnect, feedData, onClose)
  of TransportUnix:
    when not defined(windows):
      cli.loop.connectUnix(address, onConnect, feedData, onClose)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")

# ── ServerSocket ────────────────────────────────────────────────────────────

proc newServerSocket*(loop: Loop): ServerSocket =
  ServerSocket(loop: loop, clients: @[], nextId: 1, ownsLoop: false)

proc newServerSocket*(): ServerSocket =
  ## Creates a SERVER socket with its own event loop.
  ServerSocket(loop: newLoop(), clients: @[], nextId: 1, ownsLoop: true)

proc getLoop*(s: ServerSocket): Loop {.inline.} = s.loop
proc run*(s: ServerSocket) {.inline.} = s.loop.run()
proc stop*(s: ServerSocket) {.inline.} = s.loop.stop()

proc peerCount*(srv: ServerSocket): int =
  srv.clients.len

proc sendTo*(srv: ServerSocket; client: ServerClient; data: string): bool {.discardable.} =
  ## Replies with one single-frame message to the given CLIENT. Returns
  ## false when the message was not delivered.
  if client != nil and client.zc != nil and client.zc.state == ZmtpEstablished:
    return client.zc.sendMessage(data.toOpenArrayByte(0, data.high))
  false

proc sendToMultipart*(srv: ServerSocket; client: ServerClient;
                      frames: openArray[string]): bool {.discardable.} =
  ## Replies with one multipart message to the given CLIENT. Returns false
  ## unless every frame was accepted by the transport.
  if client == nil or client.zc == nil or
      client.zc.state != ZmtpEstablished: return false
  var fb = newSeq[seq[byte]](frames.len)
  for i, f in frames:
    fb[i] = newSeq[byte](f.len)
    if f.len > 0:
      copyMem(addr fb[i][0], unsafeAddr f[0], f.len)
  client.zc.sendMultipart(fb)

proc sendToId*(srv: ServerSocket; id: uint32; data: string): bool {.discardable.} =
  ## Replies to the CLIENT with the given routing id. Returns false when
  ## no connected CLIENT carries that id or the transport is dead.
  for client in srv.clients:
    if client.id == id:
      return srv.sendTo(client, data)
  false

proc close*(srv: ServerSocket) =
  for client in srv.clients:
    client.zc.close()
  srv.clients.setLen(0)
  if srv.server != nil:
    srv.server.close()
    srv.server = nil
  if srv.onClose != nil: srv.onClose()
  if srv.ownsLoop and srv.loop != nil:
    srv.loop.close()

proc dropClient(srv: ServerSocket; client: ServerClient) =
  srv.clients.keepItIf(it != client)
  if srv.clients.len == 0 and srv.onClose != nil:
    srv.onClose()

proc wireServerHandlers(srv: ServerSocket; client: ServerClient; zc: ZmtpConnection) =
  zc.onReady = proc(zc: ZmtpConnection) =
    if zc.peerSocketType != "CLIENT":
      discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
      zc.close()
      srv.dropClient(client)
  zc.onMultipart = proc(zc: ZmtpConnection; frames: seq[seq[byte]]) =
    if frames.len == 1 and srv.onRequest != nil:
      srv.onRequest(client, frames[0])
    if srv.onRequestMultipart != nil:
      srv.onRequestMultipart(client, frames)
  zc.onClose = proc(zc: ZmtpConnection) =
    srv.dropClient(client)

proc `bind`*(srv: ServerSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = srv

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    if transport == TransportTcp and not wrapServerConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "SERVER"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    applyClientServerAuth(zc, ps.authMech, ps.authPubKey, ps.authSecKey,
      ps.authSrvKey, ps.authUser, ps.authPass, ps.authZapHandler,
      ps.hasVouch, ps.authVouchPub, ps.authVouchSec, ps.requireVouch, ps.expectedVouchPub)
    let client = ServerClient(id: ps.nextId, zc: zc)
    inc ps.nextId
    ps.clients.add(client)
    wireServerHandlers(ps, client, zc)

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

proc connect*(srv: ServerSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  let ps = srv

  proc onConnect(conn: Connection) =
    if transport == TransportTcp and not wrapClientConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "SERVER"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    applyClientServerAuth(zc, ps.authMech, ps.authPubKey, ps.authSecKey,
      ps.authSrvKey, ps.authUser, ps.authPass, ps.authZapHandler,
      ps.hasVouch, ps.authVouchPub, ps.authVouchSec, ps.requireVouch, ps.expectedVouchPub)
    let client = ServerClient(id: ps.nextId, zc: zc)
    inc ps.nextId
    ps.clients.add(client)
    wireServerHandlers(ps, client, zc)

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
    srv.loop.connect(address, port, onConnect, feedData, onClose)
  of TransportUnix:
    when not defined(windows):
      srv.loop.connectUnix(address, onConnect, feedData, onClose)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")