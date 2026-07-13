## Exclusive PAIR socket pattern — one-to-one bidirectional communication.
##
## PAIR sockets provide a strict one-to-one connection between two peers.
## The server rejects any further connection attempts after the first peer
## is accepted. The communication is bidirectional and stateless.
##
## Usage:
##   ```nim
##   let loop = newLoop()
##
##   # Server side
##   let srv = newPairSocket(loop)
##   srv.bind("127.0.0.1", 5555)
##   srv.onMessage = proc(data: string) {.closure.} =
##     echo "received: ", data
##
##   # Client side
##   let cli = newPairSocket(loop)
##   cli.connect("127.0.0.1", 5555)
##   cli.send("hello")
##   ```

import powpow/[loop, types, net/tcp]
import ./zmtp

export loop, types, tcp, zmtp

type
  PairSocket* = ref object
    loop:       Loop
    server:     TcpServer
    conn*:       ZmtpConnection
    onMessage*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}

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
    let zc = initZmtpConnection(conn, asServer = true)
    zc.socketType = "PAIR"
    conn.data = cast[pointer](zc)
    ps.conn = zc  # Keep GC reference alive

    zc.onReady = proc(zc: ZmtpConnection) =
      if zc.peerSocketType != "PAIR":
        discard zc.sendCommand("ERROR", "Invalid socket type: " & zc.peerSocketType)
        zc.close()
        return
      # Connection established and handshake complete

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
    let zc = initZmtpConnection(conn, asServer = false)
    zc.socketType = "PAIR"
    conn.data = cast[pointer](zc)
    ps.conn = zc

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
