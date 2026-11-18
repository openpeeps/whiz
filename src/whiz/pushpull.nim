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
import ./tls

export loop, types, tcp, zmtp, auth, curve, tls

type
  PushSocket* = ref object
    loop:       Loop
    ownsLoop: bool
    batchWrites: bool
    server:     TcpServer
    conns:      seq[ZmtpConnection]
    rrIndex:    int
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    tls: TlsConfig
    authVouchPub: array[32, uint8]
    authVouchSec: array[64, uint8]
    hasVouch: bool
    expectedVouchPub: array[32, uint8]
    requireVouch: bool

  PullSocket* = ref object
    loop:       Loop
    ownsLoop: bool
    batchWrites: bool
    server:     TcpServer
    conns:      seq[ZmtpConnection]
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}
    authMech:   string
    authPubKey, authSecKey, authSrvKey: array[32, uint8]
    tls: TlsConfig
    authVouchPub: array[32, uint8]
    authVouchSec: array[64, uint8]
    hasVouch: bool
    expectedVouchPub: array[32, uint8]
    requireVouch: bool

proc setCurveKeypair*(pull: PullSocket; publicKey, secretKey: array[32, uint8]) =
  pull.authMech = "CURVE"
  pull.authPubKey = publicKey
  pull.authSecKey = secretKey

proc setCurveClient*(push: PushSocket; publicKey, secretKey, serverKey: array[32, uint8]) =
  push.authMech = "CURVE"
  push.authPubKey = publicKey
  push.authSecKey = secretKey
  push.authSrvKey = serverKey

proc setTlsServer*(s: PushSocket | PullSocket; certFile, keyFile: string) =
  ## Enables TLS for accepted connections (TCP only, POSIX only). Call
  ## before bind. Raises SslError when the cert and key do not match.
  enableTlsServer(s.tls, certFile, keyFile)

proc setTlsClient*(s: PushSocket | PullSocket; verifyPeer = true; serverName = "") =
  ## Enables TLS for outbound connections (TCP only, POSIX only). Call
  ## before connect. Pass verifyPeer=false for self-signed test certs.
  enableTlsClient(s.tls, verifyPeer, serverName)

proc setTlsAlpn*(s: PushSocket | PullSocket; protos: openArray[string]) =
  if s.tls != nil:
    s.tls.setTlsAlpn(protos)

proc setTlsKtlsAuto*(s: PushSocket | PullSocket; zerocopySendfile = false) =
  if s.tls == nil: s.tls = TlsConfig()
  s.tls.enableTlsKtls(zerocopySendfile)

proc setTlsKtlsRequired*(s: PushSocket | PullSocket; zerocopySendfile = false) =
  if s.tls == nil: s.tls = TlsConfig()
  s.tls.configureTlsKtls(KtlsRequired, zerocopySendfile)

proc setCurveVouch*(s: PushSocket | PullSocket; vouchPub: array[32, uint8];
                     vouchSec: array[64, uint8]) =
  s.authVouchPub = vouchPub
  s.authVouchSec = vouchSec
  s.hasVouch = true
  if s.authMech == "": s.authMech = "CURVE"

proc setCurveExpectedVouch*(s: PushSocket | PullSocket; vouchPub: array[32, uint8]) =
  s.expectedVouchPub = vouchPub
  s.requireVouch = true
  if s.authMech == "": s.authMech = "CURVE"

proc setBatching*(s: PushSocket | PullSocket; on: bool = true) =
  ## Coalesces sends into bulk writes. Enable for floods.
  s.batchWrites = on
  for zc in s.conns:
    zc.conn.setBatching(on)

proc pendingBytes*(s: PushSocket | PullSocket): int =
  var total = 0
  for zc in s.conns:
    total += zc.pendingBytes()
  total


# ── PushSocket ────────────────────────────────────────────────────────────────

proc newPushSocket*(loop: Loop): PushSocket =
  PushSocket(loop: loop, conns: @[], rrIndex: 0, ownsLoop: false)

proc newPushSocket*(): PushSocket =
  ## Creates a PUSH socket with its own event loop.
  PushSocket(loop: newLoop(), conns: @[], rrIndex: 0, ownsLoop: true)

proc getLoop*(s: PushSocket): Loop {.inline.} = s.loop
proc run*(s: PushSocket) {.inline.} = s.loop.run()
proc stop*(s: PushSocket) {.inline.} = s.loop.stop()

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
  if push.ownsLoop and push.loop != nil:
    push.loop.close()

proc `bind`*(push: PushSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = push

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    if transport == TransportTcp and not wrapServerConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "PUSH"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
      if ps.hasVouch:
        curve.setCurveVouch(zc, ps.authVouchPub, ps.authVouchSec)
      if ps.requireVouch:
        curve.setCurveExpectedVouch(zc, ps.expectedVouchPub)
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
    if transport == TransportTcp and not wrapClientConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "PUSH"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
      if ps.hasVouch:
        curve.setCurveVouch(zc, ps.authVouchPub, ps.authVouchSec)
      if ps.requireVouch:
        curve.setCurveExpectedVouch(zc, ps.expectedVouchPub)
      if ps.authSrvKey != default(array[32, uint8]):
        curve.setCurveServerKey(zc, ps.authSrvKey)
      if ps.hasVouch:
        curve.setCurveVouch(zc, ps.authVouchPub, ps.authVouchSec)
      if ps.requireVouch:
        curve.setCurveExpectedVouch(zc, ps.expectedVouchPub)
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
  PullSocket(loop: loop, conns: @[], ownsLoop: false)

proc newPullSocket*(): PullSocket =
  ## Creates a PULL socket with its own event loop.
  PullSocket(loop: newLoop(), conns: @[], ownsLoop: true)

proc getLoop*(s: PullSocket): Loop {.inline.} = s.loop
proc run*(s: PullSocket) {.inline.} = s.loop.run()
proc stop*(s: PullSocket) {.inline.} = s.loop.stop()

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
  if pull.ownsLoop and pull.loop != nil:
    pull.loop.close()

proc `bind`*(pull: PullSocket; address: string; port: int = 0;
             transport: Transport = TransportTcp) =
  let ps = pull

  proc feedData(conn: Connection; data: openArray[byte]) =
    let zc = cast[ZmtpConnection](conn.data)
    if zc != nil: zc.feed(data)

  proc onAccept(conn: Connection) =
    if transport == TransportTcp and not wrapServerConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = true, mech)
    zc.socketType = "PULL"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
      if ps.hasVouch:
        curve.setCurveVouch(zc, ps.authVouchPub, ps.authVouchSec)
      if ps.requireVouch:
        curve.setCurveExpectedVouch(zc, ps.expectedVouchPub)

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
    if transport == TransportTcp and not wrapClientConn(conn, ps.tls): return
    let mech = if ps.authMech.len > 0: ps.authMech else: "NULL"
    let zc = initZmtpConnection(conn, asServer = false, mech)
    zc.socketType = "PULL"
    conn.data = cast[pointer](zc)
    applyConnBatching(ps, zc)
    if ps.authMech == "CURVE":
      curve.setCurveKeypair(zc, ps.authPubKey, ps.authSecKey)
      if ps.hasVouch:
        curve.setCurveVouch(zc, ps.authVouchPub, ps.authVouchSec)
      if ps.requireVouch:
        curve.setCurveExpectedVouch(zc, ps.expectedVouchPub)
      if ps.authSrvKey != default(array[32, uint8]):
        curve.setCurveServerKey(zc, ps.authSrvKey)
      if ps.hasVouch:
        curve.setCurveVouch(zc, ps.authVouchPub, ps.authVouchSec)
      if ps.requireVouch:
        curve.setCurveExpectedVouch(zc, ps.expectedVouchPub)
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
