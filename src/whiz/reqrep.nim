## REQ/REP — Request-Reply socket pattern.
##
## Implements strict request-reply alternation per RFC 28/REQREP.
## A REP socket binds and waits for requests. A REQ socket connects
## and sends requests. Each REQ must receive a reply before sending
## another request. Each REP must receive a request before sending a reply.
##
## Usage:
##   ```nim
##   let loop = newLoop()
##
##   # Server (REP)
##   let rep = newRepSocket(loop)
##   rep.bind("127.0.0.1", 5555)
##   rep.onRequest = proc(data: string) {.closure.} =
##     rep.send("echo: " & data)
##
##   # Client (REQ)
##   let req = newReqSocket(loop)
##   req.connect("127.0.0.1", 5555)
##   req.onReply = proc(data: string) {.closure.} =
##     echo "got: ", data
##   req.send("hello")
##   ```

import powpow/[loop, types, net/tcp]
import ./zmtp

export loop, types, tcp, zmtp

type
  ReqSocket* = ref object
    loop:       Loop
    conn:       ZmtpConnection
    waiting:    bool             # true while awaiting a reply
    onReply*:   proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}

  RepSocket* = ref object
    loop:       Loop
    conn:       ZmtpConnection
    server:     TcpServer
    hasRequest: bool             # true while processing a request
    onRequest*: proc(data: openArray[byte]) {.closure.}
    onClose*:   proc() {.closure.}

# ── REQ ─────────────────────────────────────────────────────────────────────

proc newReqSocket*(loop: Loop): ReqSocket =
  ReqSocket(loop: loop)

proc send*(req: ReqSocket; data: string) =
  if req.conn != nil and req.conn.state == ZmtpEstablished and not req.waiting:
    req.waiting = true
    req.conn.sendMessage(data.toOpenArrayByte(0, data.high))

proc close*(req: ReqSocket) =
  if req.conn != nil:
    req.conn.close()
    req.conn = nil
  if req.onClose != nil: req.onClose()

proc connect*(req: ReqSocket; address: string; port: int = 0;
              transport: Transport = TransportTcp) =
  let ps = req

  proc onConnect(conn: Connection) =
    let zc = initZmtpConnection(conn, asServer = false)
    zc.socketType = "REQ"
    conn.data = cast[pointer](zc)
    ps.conn = zc

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

proc send*(rep: RepSocket; data: string) =
  if rep.conn != nil and rep.conn.state == ZmtpEstablished and rep.hasRequest:
    rep.hasRequest = false
    rep.conn.sendMessage(data.toOpenArrayByte(0, data.high))

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
    let zc = initZmtpConnection(conn, asServer = true)
    zc.socketType = "REP"
    conn.data = cast[pointer](zc)
    ps.conn = zc

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

  ps.server = newTcpServer(rep.loop, onAccept = onAccept, onData = feedData)
  case transport
  of TransportTcp:
    ps.server.listen(address, port)
  of TransportUnix:
    when not defined(windows):
      ps.server.listenUnix(address)
    else:
      raise newException(NetError, "Unix sockets not supported on Windows")
