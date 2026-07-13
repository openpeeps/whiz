## PUSH/PULL — Pipeline socket pattern.
##
## PUSH distributes messages to connected PULL sockets using round-robin
## scheduling. PULL receives messages from connected PUSH sockets using
## fair-queuing (messages are interleaved from all connected peers).
##
## Usage:
##   ```nim
##   let loop = newLoop()
##
##   # Worker (PULL)
##   let pull = newPullSocket(loop)
##   pull.bind("127.0.0.1", 5555)
##   pull.onMessage = proc(data: string) {.closure.} =
##     echo "work: ", data
##
##   # Pusher (PUSH)
##   let push = newPushSocket(loop)
##   push.connect("127.0.0.1", 5555)
##   push.send("task 1")
##   push.send("task 2")
##   ```
##
## Multiple PUSH senders and multiple PULL workers are supported.
## Messages from a single PUSH are distributed round-robin across
## connected PULL sockets on the send side.

import std/[sequtils]
import powpow/[loop, types, net/tcp]
import ./zmtp

export loop, types, tcp, zmtp

type
  PushSocket* = ref object
    loop:       Loop
    server:     TcpServer
    conns:      seq[ZmtpConnection]
    rrIndex:    int
    onClose*:   proc() {.closure.}

  PullSocket* = ref object
    loop:       Loop
    server:     TcpServer
    conns:      seq[ZmtpConnection]
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}

# ── PushSocket ────────────────────────────────────────────────────────────────

proc newPushSocket*(loop: Loop): PushSocket =
  PushSocket(loop: loop, conns: @[], rrIndex: 0)

proc send*(push: PushSocket; data: string) =
  if push.conns.len == 0: return
  if push.rrIndex >= push.conns.len: push.rrIndex = 0
  let idx = push.rrIndex
  push.rrIndex = (push.rrIndex + 1) mod push.conns.len
  let zc = push.conns[idx]
  if zc.state == ZmtpEstablished:
    zc.sendMessage(data.toOpenArrayByte(0, data.high))

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
    let zc = initZmtpConnection(conn, asServer = true)
    zc.socketType = "PUSH"
    conn.data = cast[pointer](zc)
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

  ps.server = newTcpServer(ps.loop, onAccept = onAccept, onData = feedData)
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
    let zc = initZmtpConnection(conn, asServer = false)
    zc.socketType = "PUSH"
    conn.data = cast[pointer](zc)
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

proc send*(pull: PullSocket; data: string) =
  ## Send a message back to the most recent sender. In a standard PULL
  ## socket this is atypical — PULL is receive-only — but we provide it
  ## for symmetry and for pipeline patterns where workers need to reply.
  if pull.conns.len == 0: return
  let zc = pull.conns[^1]
  if zc.state == ZmtpEstablished:
    zc.sendMessage(data.toOpenArrayByte(0, data.high))

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
    let zc = initZmtpConnection(conn, asServer = true)
    zc.socketType = "PULL"
    conn.data = cast[pointer](zc)

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

  ps.server = newTcpServer(ps.loop, onAccept = onAccept, onData = feedData)
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
    let zc = initZmtpConnection(conn, asServer = false)
    zc.socketType = "PULL"
    conn.data = cast[pointer](zc)
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
