## ZMTP 3.0 — ZeroMQ Message Transport Protocol implementation.
##
## Implements the wire protocol defined by RFC 23/ZMTP:
##   - 64-byte greeting exchange with version negotiation
##   - Short/long frame encoding (flags + size + body)
##   - NULL security mechanism (READY/ERROR commands)
##   - SUBSCRIBE/UNSUBSCRIBE command frames for PUB/SUB
##
## Built on top of powpow's TCP Connection primitives.

when defined(windows):
  import std/winlean
else:
  import std/posix

import std/[tables]
import powpow/[loop, types, net/tcp]

export tcp, loop, types

type
  Transport* = enum
    TransportTcp
    TransportUnix

const
  ZmtpSignature*  = 0xFF
  ZmtpMarker*     = 0x7F
  ZmtpMajor*      = 3
  ZmtpMinor*      = 0
  ZmtpMechanismLen = 20

  ZmtpMore*    = 0x01
  ZmtpLong*    = 0x02
  ZmtpCommand* = 0x04

type
  ZmtpState* = enum
    ZmtpGreeting
    ZmtpReady
    ZmtpEstablished
    ZmtpClosed

  ZmtpConnection* = ref object
    conn*:        Connection
    state*:       ZmtpState
    socketType*:  string
    peerSocketType*: string
    identity*:    string
    mechanism*:   string
    recvBuf:      seq[byte]
    recvOff:      int
    recvLen:      int
    onReady*:     proc(zc: ZmtpConnection) {.closure.}
    onMessage*:   proc(zc: ZmtpConnection; data: openArray[byte]) {.closure.}
    onSubscribe*: proc(zc: ZmtpConnection; topic: openArray[byte]) {.closure.}
    onUnsubscribe*: proc(zc: ZmtpConnection; topic: openArray[byte]) {.closure.}
    onError*:     proc(zc: ZmtpConnection; reason: string) {.closure.}
    onClose*:     proc(zc: ZmtpConnection) {.closure.}

# ── Greeting ─────────────────────────────────────────────────────────────────

proc buildGreeting*(asServer: bool): array[64, byte] =
  result[0] = ZmtpSignature
  result[9] = ZmtpMarker
  result[10] = ZmtpMajor
  result[11] = ZmtpMinor
  let mech = "NULL"
  for i, c in mech: result[12 + i] = byte(c)
  result[32] = byte(asServer.ord)

proc parseGreeting(buf: ptr UncheckedArray[byte]): tuple[ok: bool; mechanism: string] =
  if buf[0] != ZmtpSignature or buf[9] != ZmtpMarker:
    return (false, "")
  var mechLen = 0
  while mechLen < ZmtpMechanismLen and buf[12 + mechLen] != 0:
    inc mechLen
  if mechLen == 0:
    return (false, "")
  result.mechanism = cast[string](newString(mechLen))
  copyMem(addr result.mechanism[0], addr buf[12], mechLen)
  result.ok = true

# ── Frame I/O ────────────────────────────────────────────────────────────────

proc sendFrame*(zc: ZmtpConnection; flags: byte; body: openArray[byte]): int =
  if zc.conn == nil: return -1
  if body.len <= 255:
    var hdr: array[2, byte]
    hdr[0] = flags
    hdr[1] = byte(body.len)
    if body.len > 0:
      result = zc.conn.sendv([
        (cast[ptr UncheckedArray[byte]](addr hdr[0]), 2),
        (cast[ptr UncheckedArray[byte]](unsafeAddr body[0]), body.len)
      ])
    else:
      result = zc.conn.send(hdr)
  else:
    var hdr: array[10, byte]
    hdr[0] = flags or ZmtpLong
    let blen = uint64(body.len)
    for i in 0 ..< 8:
      hdr[1 + i] = byte((blen shr ((7 - i) * 8)) and 0xFF)
    if body.len > 0:
      result = zc.conn.sendv([
        (cast[ptr UncheckedArray[byte]](addr hdr[0]), 10),
        (cast[ptr UncheckedArray[byte]](unsafeAddr body[0]), body.len)
      ])
    else:
      result = zc.conn.send(hdr)

proc sendCommand*(zc: ZmtpConnection; name: string; data: openArray[byte] = @[]): int =
  var body = newSeq[byte](name.len + 1 + data.len)
  copyMem(addr body[0], unsafeAddr name[0], name.len)
  body[name.len] = 0
  if data.len > 0:
    copyMem(addr body[name.len + 1], unsafeAddr data[0], data.len)
  result = zc.sendFrame(ZmtpCommand, body)

proc sendCommand*(zc: ZmtpConnection; name: string; data: string): int =
  zc.sendCommand(name, data.toOpenArrayByte(0, data.high))

proc sendReady*(zc: ZmtpConnection; socketType: string) =
  var body = newSeq[byte]()
  let stProp = "Socket-Type"
  body.add(byte(stProp.len))
  for c in stProp: body.add(byte(c))
  body.add(byte((socketType.len shr 24) and 0xFF))
  body.add(byte((socketType.len shr 16) and 0xFF))
  body.add(byte((socketType.len shr 8) and 0xFF))
  body.add(byte(socketType.len and 0xFF))
  for c in socketType: body.add(byte(c))
  discard zc.sendCommand("READY", body)

proc sendMessage*(zc: ZmtpConnection; data: openArray[byte]) =
  discard zc.sendFrame(0, data)

proc close*(zc: ZmtpConnection) =
  if zc == nil: return
  zc.state = ZmtpClosed
  if zc.conn != nil:
    zc.conn.close()

# ── Parser ───────────────────────────────────────────────────────────────────

template consume(zc: ZmtpConnection; n: int) =
  zc.recvOff += n
  zc.recvLen -= n
  if zc.recvLen == 0:
    zc.recvOff = 0

proc parseReadyProps(zc: ZmtpConnection; buf: ptr byte; len: int) =
  let d = cast[ptr UncheckedArray[byte]](buf)
  var i = 0
  while i < len:
    let propNameLen = d[i].int; inc i
    if i + propNameLen > len: break
    let propName = cast[string](newString(propNameLen))
    copyMem(addr propName[0], addr d[i], propNameLen); i += propNameLen
    if i + 4 > len: break
    let propValLen = (d[i].int shl 24) or (d[i+1].int shl 16) or
                     (d[i+2].int shl 8) or d[i+3].int; i += 4
    if i + propValLen > len: break
    if propName == "Socket-Type":
      zc.peerSocketType = cast[string](newString(propValLen))
      copyMem(addr zc.peerSocketType[0], addr d[i], propValLen)
    elif propName == "Identity" and propValLen > 0:
      zc.identity = cast[string](newString(propValLen))
      copyMem(addr zc.identity[0], addr d[i], propValLen)
    i += propValLen

proc handleCommand(zc: ZmtpConnection; buf: ptr byte; size: int) =
  let d = cast[ptr UncheckedArray[byte]](buf)
  var nullPos = 0
  while nullPos < size and d[nullPos] != 0: inc nullPos
  if nullPos == 0: return
  let cmdName = cast[string](newString(nullPos))
  copyMem(addr cmdName[0], addr d[0], nullPos)
  let dataLen = size - nullPos - 1

  case cmdName
  of "READY":
    if dataLen > 0:
      parseReadyProps(zc, addr d[nullPos + 1], dataLen)
    if zc.state == ZmtpReady:
      zc.state = ZmtpEstablished
      if zc.onReady != nil: zc.onReady(zc)
  of "SUBSCRIBE":
    if dataLen >= 1 and d[nullPos + 1] == 1:
      var topic: seq[byte]
      if dataLen > 1:
        topic = newSeq[byte](dataLen - 1)
        copyMem(addr topic[0], addr d[nullPos + 2], dataLen - 1)
      if zc.onSubscribe != nil:
        var zc2 = zc
        zc2.onSubscribe(zc2, topic)
  of "UNSUBSCRIBE":
    if dataLen >= 1 and d[nullPos + 1] == 0:
      var topic: seq[byte]
      if dataLen > 1:
        topic = newSeq[byte](dataLen - 1)
        copyMem(addr topic[0], addr d[nullPos + 2], dataLen - 1)
      if zc.onUnsubscribe != nil: zc.onUnsubscribe(zc, topic)
  of "ERROR":
    if dataLen > 0:
      let reason = cast[string](newString(dataLen))
      copyMem(addr reason[0], addr d[nullPos + 1], dataLen)
      if zc.onError != nil: zc.onError(zc, reason)
    zc.state = ZmtpClosed
  else: discard

proc readFrame(zc: ZmtpConnection; buf: ptr byte; rlen: int): tuple[ok: bool; flags: byte; body: ptr byte; bodyLen: int; consumed: int] =
  if rlen < 2: return (false, 0, nil, 0, 0)
  let b = cast[ptr UncheckedArray[byte]](buf)
  let flags = b[0]
  if (flags and ZmtpLong) != 0:
    if rlen < 10: return (false, 0, nil, 0, 0)
    var size = 0
    for i in 0 ..< 8: size = (size shl 8) or b[1 + i].int
    if rlen < 10 + size: return (false, 0, nil, 0, 0)
    result = (true, flags, addr b[10], size, 10 + size)
  else:
    let size = b[1].int
    if rlen < 2 + size: return (false, 0, nil, 0, 0)
    result = (true, flags, addr b[2], size, 2 + size)

proc feed*(zc: ZmtpConnection; data: openArray[byte]) =
  if zc.state == ZmtpClosed: return
  let writePos = zc.recvOff + zc.recvLen
  let spaceAtEnd = zc.recvBuf.len - writePos
  if data.len > spaceAtEnd:
    if zc.recvOff + zc.recvLen + data.len <= zc.recvBuf.len:
      copyMem(addr zc.recvBuf[0], addr zc.recvBuf[zc.recvOff], zc.recvLen)
      zc.recvOff = 0
    else:
      zc.recvBuf.setLen(writePos + data.len)
  copyMem(addr zc.recvBuf[zc.recvOff + zc.recvLen], unsafeAddr data[0], data.len)
  zc.recvLen += data.len

  var done = false
  while not done:
    let buf = addr zc.recvBuf[zc.recvOff]
    case zc.state
    of ZmtpGreeting:
      if zc.recvLen < 64:
        done = true
      else:
        let (ok, mechanism) = parseGreeting(cast[ptr UncheckedArray[byte]](buf))
        if not ok or mechanism != "NULL":
          if zc.onError != nil: zc.onError(zc, "Invalid greeting")
          zc.state = ZmtpClosed
          done = true
        else:
          zc.consume(64)
          zc.state = ZmtpReady
          if zc.socketType.len > 0:
            zc.sendReady(zc.socketType)

    of ZmtpReady:
      let (ok, flags, body, bodyLen, consumed) = readFrame(zc, buf, zc.recvLen)
      if not ok:
        done = true
      elif (flags and ZmtpCommand) == 0:
        done = true
      else:
        if body != nil and bodyLen > 0:
          handleCommand(zc, body, bodyLen)
        zc.consume(consumed)

    of ZmtpEstablished:
      let (ok, flags, body, bodyLen, consumed) = readFrame(zc, buf, zc.recvLen)
      if not ok:
        done = true
      elif (flags and ZmtpCommand) != 0:
        if body != nil and bodyLen > 0:
          handleCommand(zc, body, bodyLen)
        zc.consume(consumed)
      else:
        if body != nil and bodyLen > 0:
          if zc.onMessage != nil:
            zc.onMessage(zc, cast[ptr UncheckedArray[byte]](body).toOpenArray(0, bodyLen - 1))
        zc.consume(consumed)

    of ZmtpClosed:
      done = true

# ── Connection setup ─────────────────────────────────────────────────────────

proc initZmtpConnection*(conn: Connection; asServer: bool): ZmtpConnection =
  result = ZmtpConnection(
    conn: conn,
    state: ZmtpGreeting,
    recvBuf: newSeq[byte](4096),
    mechanism: "NULL",
    recvOff: 0,
  )
  discard result.conn.send(buildGreeting(asServer))
