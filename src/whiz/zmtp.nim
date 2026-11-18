# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## ZMTP 3.0 — ZeroMQ Message Transport Protocol implementation.
##
## Implements the wire protocol defined by RFC 23/ZMTP:
##   - 64-byte greeting exchange with version negotiation
##   - Short/long frame encoding (flags + size + body)
##   - NULL, PLAIN, CURVE security mechanisms
##   - SUBSCRIBE/UNSUBSCRIBE command frames for PUB/SUB
##   - Mechanism dispatch with encrypt/decrypt hooks
##
## Built on top of powpow's TCP Connection primitives.

when defined(windows):
  import std/winlean
else:
  import std/posix

import std/[tables]
import powpow/[loop, types, net/tcp]
import buffalo

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

  # Upper bound for one reassembled multipart message. Connections that
  # exceed it are closed with an error instead of growing unboundedly.
  MaxMultipartBytes* = 32 * 1024 * 1024

type
  ZmtpState* = enum
    ZmtpGreeting
    ZmtpHandshake
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
    asServer*:    bool
    recvBuf:      seq[byte]
    recvOff:      int
    recvLen:      int
    onReady*:     proc(zc: ZmtpConnection) {.closure.}
    onMessage*:   proc(zc: ZmtpConnection; data: openArray[byte]) {.closure.}
    # Opt-in multipart delivery. When set, incoming data frames are
    # accumulated while ZmtpMore is present and delivered as one message
    # once the final frame arrives (single-frame messages arrive as a
    # one-element seq). When nil, each frame goes to onMessage as before.
    onMultipart*: proc(zc: ZmtpConnection; frames: seq[seq[byte]]) {.closure.}
    pendingFrames: seq[seq[byte]]
    pendingBytes: int
    onSubscribe*: proc(zc: ZmtpConnection; topic: openArray[byte]) {.closure.}
    onUnsubscribe*: proc(zc: ZmtpConnection; topic: openArray[byte]) {.closure.}
    onError*:     proc(zc: ZmtpConnection; reason: string) {.closure.}
    onClose*:     proc(zc: ZmtpConnection) {.closure.}
    # Mechanism dispatch
    mechHandshake*: proc(zc: ZmtpConnection; body: openArray[byte]): bool {.closure.}
    mechEncrypt*: proc(zc: ZmtpConnection; data: var seq[byte]): bool {.closure.}
    mechDecrypt*: proc(zc: ZmtpConnection; data: var seq[byte]): bool {.closure.}
    mechDestroy*: proc(zc: ZmtpConnection) {.closure.}
    mechData*:     RootRef  # mechanism-specific state
    # Non-nil when ZMTP frames are tunnelled through another transport (e.g. WebSocket).
    # Called with raw ZMTP frame bytes (header + body) to send through the tunnel.
    # The view borrows a per-connection scratch buffer: copy it if you must
    # retain bytes past the call (the default WS bridge copies synchronously).
    sendOverride*: proc(zc: ZmtpConnection; data: openArray[byte]): int {.closure.}
    # Reusable scratch for the sendOverride framing path (buffalo pool:
    # grows once, then stays flat — no per-frame seq allocation).
    sendScratch: PooledBuffer[byte]

# ── Greeting ─────────────────────────────────────────────────────────────────

proc buildGreeting*(asServer: bool; mechanism: string = "NULL"): array[64, byte] =
  result[0] = ZmtpSignature
  result[9] = ZmtpMarker
  result[10] = ZmtpMajor
  result[11] = ZmtpMinor
  for i, c in mechanism: result[12 + i] = byte(c)
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
  ## Send a ZMTP frame. When mechEncrypt is nil the body is referenced
  ## directly (zero-copy through the framing layer). When encryption is
  ## active the body is copied, encrypted in-place, and the ciphertext is sent.
  if zc.conn == nil: return -1
  if zc.sendOverride != nil:
    # Scratch-reused framing: header + body are blitted into the
    # per-connection pool (no per-frame seq alloc). The view is only valid
    # for the call — sendOverride must copy synchronously (the WS bridge
    # does: powpow queues bytes before returning).
    # NOTE: setLen (not ensureLen) — the view length must be exact, since
    # toOpenArray spans b.len and a stale longer len would leak bytes
    # from previous (e.g. handshake) frames into this one.
    let hdrLen = if body.len <= 255: 2 else: 10
    let total = hdrLen + body.len
    zc.sendScratch.setLen(total)
    let base = cast[ptr UncheckedArray[byte]](addr zc.sendScratch.data[0])
    if body.len <= 255:
      base[0] = flags
      base[1] = byte(body.len)
    else:
      base[0] = flags or ZmtpLong
      let blen = uint64(body.len)
      for i in 0 ..< 8:
        base[1 + i] = byte((blen shr ((7 - i) * 8)) and 0xFF)
    if body.len > 0:
      copyMem(addr base[hdrLen], unsafeAddr body[0], body.len)
    return zc.sendOverride(zc, zc.sendScratch.toOpenArray())
  if zc.mechEncrypt != nil:
    var payload = @body
    if not zc.mechEncrypt(zc, payload): return -1
    if payload.len <= 255:
      var hdr: array[2, byte]
      hdr[0] = flags
      hdr[1] = byte(payload.len)
      if payload.len > 0:
        result = zc.conn.sendv([
          (cast[ptr UncheckedArray[byte]](addr hdr[0]), 2),
          (cast[ptr UncheckedArray[byte]](addr payload[0]), payload.len)
        ])
      else:
        result = zc.conn.send(hdr)
    else:
      var hdr: array[10, byte]
      hdr[0] = flags or ZmtpLong
      let blen = uint64(payload.len)
      for i in 0 ..< 8:
        hdr[1 + i] = byte((blen shr ((7 - i) * 8)) and 0xFF)
      if payload.len > 0:
        result = zc.conn.sendv([
          (cast[ptr UncheckedArray[byte]](addr hdr[0]), 10),
          (cast[ptr UncheckedArray[byte]](addr payload[0]), payload.len)
        ])
      else:
        result = zc.conn.send(hdr)
  else:
    # Zero-copy path — reference body directly
    if body.len <= 255:
      if body.len <= 64:
        # Single-send fast path for tiny frames: one contiguous stack
        # buffer instead of a 2-part writev (skips iovec setup).
        var buf: array[66, byte]
        buf[0] = flags
        buf[1] = byte(body.len)
        if body.len > 0:
          copyMem(addr buf[2], unsafeAddr body[0], body.len)
        result = zc.conn.send(buf.toOpenArray(0, body.len + 1))
      else:
        var hdr: array[2, byte]
        hdr[0] = flags
        hdr[1] = byte(body.len)
        result = zc.conn.sendv([
          (cast[ptr UncheckedArray[byte]](addr hdr[0]), 2),
          (cast[ptr UncheckedArray[byte]](unsafeAddr body[0]), body.len)
        ])
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

proc sendMessage*(zc: ZmtpConnection; data: openArray[byte]): bool {.discardable.} =
  ## Returns true when the frame was accepted by the transport. False means
  ## the underlying connection is dead or errored — the message was not sent.
  zc.sendFrame(0, data) > 0

proc toStr*(data: openArray[byte]): string =
  ## Copies a received frame payload into a string. Handy for examples
  ## and tests; zero-copy handlers receive `openArray[byte]`.
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

template applyConnOpts*(ps, zc) =
  ## Applies socket-level connection options to a freshly set-up
  ## ZmtpConnection: write batching (`setBatching`) and kernel socket
  ## buffer hints (`setSendBufferSize`/`setRecvBufferSize`). No-op unless
  ## the socket enabled them.
  if ps.batchWrites:
    zc.conn.setBatching(true)
  if ps.sndBufSize > 0:
    zc.conn.setSendBufferSize(ps.sndBufSize)
  if ps.rcvBufSize > 0:
    zc.conn.setRecvBufferSize(ps.rcvBufSize)

proc sendMultipart*(zc: ZmtpConnection; frames: openArray[seq[byte]]): bool {.discardable.} =
  ## Sends a multipart message. Every frame but the last carries ZmtpMore.
  ## Returns true only when every frame was accepted by the transport.
  if zc.state != ZmtpEstablished: return false
  for i, f in frames:
    let flags = if i < frames.len - 1: ZmtpMore.byte else: 0.byte
    if zc.sendFrame(flags, f) <= 0: return false
  true

proc close*(zc: ZmtpConnection) =
  if zc == nil: return
  zc.state = ZmtpClosed
  if zc.mechDestroy != nil:
    zc.mechDestroy(zc)
    zc.mechDestroy = nil
  if zc.conn != nil:
    zc.conn.close()

# ── Parser ───────────────────────────────────────────────────────────────────

template consume(zc: ZmtpConnection; n: int) =
  zc.recvOff += n
  zc.recvLen -= n
  if zc.recvLen == 0:
    zc.recvOff = 0

template establishedFrame(zc: ZmtpConnection; flags: byte; body: ptr byte;
                          bodyLen, consumed: int; commit: untyped) =
  ## Shared ZmtpEstablished frame handling for both the staging path
  ## (`commit` = consume from recvBuf) and the zero-copy fast path
  ## (`commit` = advance the read-chunk cursor). Bodies are either copied
  ## into owned seqs (multipart accumulation, decryption) or delivered as
  ## transient views valid only for the callback — same contract as before.
  if (flags and ZmtpCommand) != 0:
    if body != nil and bodyLen > 0:
      handleCommand(zc, body, bodyLen)
    commit
  elif zc.onMultipart != nil:
    var frame = newSeq[byte](bodyLen)
    if bodyLen > 0:
      copyMem(addr frame[0], body, bodyLen)
      if zc.mechDecrypt != nil and not zc.mechDecrypt(zc, frame):
        if zc.onError != nil: zc.onError(zc, "Decryption failed")
        zc.state = ZmtpClosed
    if zc.state != ZmtpClosed:
      zc.pendingFrames.add(frame)
      zc.pendingBytes += bodyLen
      if zc.pendingBytes > MaxMultipartBytes:
        if zc.onError != nil: zc.onError(zc, "Multipart message too large")
        zc.state = ZmtpClosed
      elif (flags and ZmtpMore) == 0:
        let msg = zc.pendingFrames
        zc.pendingFrames = @[]
        zc.pendingBytes = 0
        zc.onMultipart(zc, msg)
    commit
  else:
    if body != nil and bodyLen > 0:
      let raw = cast[ptr UncheckedArray[byte]](body)
      if zc.mechDecrypt != nil:
        var msg = newSeq[byte](bodyLen)
        copyMem(addr msg[0], raw, bodyLen)
        if not zc.mechDecrypt(zc, msg):
          if zc.onError != nil: zc.onError(zc, "Decryption failed")
          zc.state = ZmtpClosed
        elif zc.onMessage != nil:
          zc.onMessage(zc, msg)
      else:
        if zc.onMessage != nil:
          zc.onMessage(zc, raw.toOpenArray(0, bodyLen - 1))
    commit

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
    if dataLen >= 1 and d[nullPos + 1] == 1 and zc.onSubscribe != nil:
      var zc2 = zc
      if dataLen > 1:
        var topic = newSeq[byte](dataLen - 1)
        copyMem(addr topic[0], addr d[nullPos + 2], dataLen - 1)
        zc2.onSubscribe(zc2, topic)
      else:
        zc2.onSubscribe(zc2, @[])
  of "UNSUBSCRIBE":
    if dataLen >= 1 and d[nullPos + 1] == 0 and zc.onUnsubscribe != nil:
      if dataLen > 1:
        var topic = newSeq[byte](dataLen - 1)
        copyMem(addr topic[0], addr d[nullPos + 2], dataLen - 1)
        zc.onUnsubscribe(zc, topic)
      else:
        zc.onUnsubscribe(zc, @[])
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
  if zc.state == ZmtpClosed or data.len == 0: return
  if zc.state == ZmtpEstablished:
    # Zero-copy receive: parse directly from the read chunk. Nothing is
    # staged except a trailing partial frame, and a split frame is
    # completed by copying only its missing tail (at most one frame),
    # never the whole chunk. Delivered views borrow `data` (fast path)
    # or the reused `recvBuf` (split frame) and are valid only for the
    # callback, same contract as before.
    var off = 0
    if zc.recvLen > 0:
      if zc.recvOff > 0:
        if zc.recvLen > 0:
          copyMem(addr zc.recvBuf[0], addr zc.recvBuf[zc.recvOff], zc.recvLen)
        zc.recvOff = 0
      while zc.recvLen > 0 and off < data.len and zc.state == ZmtpEstablished:
        if zc.recvLen < 1: break
        let sflags = zc.recvBuf[0]
        var total = -1
        if (sflags and ZmtpLong) == 0:
          if zc.recvLen >= 2:
            total = 2 + zc.recvBuf[1].int
          elif off < data.len:
            total = 2 + data[off].int
          else: break
        else:
          if zc.recvLen >= 10:
            var sz = 0
            for i in 0 ..< 8: sz = (sz shl 8) or zc.recvBuf[1 + i].int
            total = 10 + sz
          else:
            if zc.recvLen + (data.len - off) < 10:
              let rem = data.len - off
              if zc.recvBuf.len < zc.recvLen + rem:
                zc.recvBuf.setLen(zc.recvLen + rem)
              copyMem(addr zc.recvBuf[zc.recvLen], unsafeAddr data[off], rem)
              zc.recvLen += rem
              return
            let hneed = 10 - zc.recvLen
            if zc.recvBuf.len < 10: zc.recvBuf.setLen(10)
            copyMem(addr zc.recvBuf[zc.recvLen], unsafeAddr data[off], hneed)
            zc.recvLen = 10
            off += hneed
            continue
        if total < 0: break
        let need = total - zc.recvLen
        if need <= 0:
          # Staging already holds a full frame; deliver it without new data.
          let (ok2, flags2, body2, bodyLen2, consumed2) =
            readFrame(zc, addr zc.recvBuf[0], zc.recvLen)
          if not ok2: return
          establishedFrame(zc, flags2, body2, bodyLen2, consumed2,
                           zc.consume(consumed2))
          if zc.state != ZmtpEstablished: return
          continue
        if off + need > data.len:
          let rem = data.len - off
          if zc.recvBuf.len < zc.recvLen + rem:
            zc.recvBuf.setLen(zc.recvLen + rem)
          copyMem(addr zc.recvBuf[zc.recvLen], unsafeAddr data[off], rem)
          zc.recvLen += rem
          return
        if zc.recvBuf.len < total: zc.recvBuf.setLen(total)
        copyMem(addr zc.recvBuf[zc.recvLen], unsafeAddr data[off], need)
        zc.recvLen = total
        off += need
        let (ok3, flags3, body3, bodyLen3, consumed3) =
          readFrame(zc, addr zc.recvBuf[0], zc.recvLen)
        if not ok3: return
        establishedFrame(zc, flags3, body3, bodyLen3, consumed3,
                         zc.consume(consumed3))
        if zc.state != ZmtpEstablished: return
      if zc.recvLen > 0: return
      if zc.state != ZmtpEstablished: return
    var foff = off
    while foff < data.len and zc.state == ZmtpEstablished:
      let (ok, flags, body, bodyLen, consumed) =
        readFrame(zc, cast[ptr byte](unsafeAddr data[foff]), data.len - foff)
      if not ok: break
      establishedFrame(zc, flags, body, bodyLen, consumed, foff += consumed)
    if zc.state != ZmtpEstablished:
      return # ERROR command closed us; drop the rest like `done` would
    if foff < data.len:
      let rem = data.len - foff
      if zc.recvBuf.len < rem:
        zc.recvBuf.setLen(rem)
      copyMem(addr zc.recvBuf[0], unsafeAddr data[foff], rem)
      zc.recvOff = 0
      zc.recvLen = rem
    elif zc.recvBuf.len > 8192:
      zc.recvBuf.setLen(4096)
    return
  if zc.recvOff > 0:
    if zc.recvLen > 0:
      copyMem(addr zc.recvBuf[0], addr zc.recvBuf[zc.recvOff], zc.recvLen)
    zc.recvOff = 0
  let spaceAtEnd = zc.recvBuf.len - zc.recvLen
  if data.len > spaceAtEnd:
    zc.recvBuf.setLen(zc.recvLen + data.len)
  copyMem(addr zc.recvBuf[zc.recvLen], unsafeAddr data[0], data.len)
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
        if not ok:
          if zc.onError != nil: zc.onError(zc, "Invalid greeting signature")
          zc.state = ZmtpClosed
          done = true
        else:
          zc.consume(64)
          zc.mechanism = mechanism
          case mechanism
          of "NULL":
            zc.state = ZmtpReady
            if zc.socketType.len > 0:
              zc.sendReady(zc.socketType)
          of "PLAIN":
            zc.state = ZmtpReady
            if not zc.asServer and zc.mechHandshake != nil:
              discard zc.mechHandshake(zc, @[])
          of "CURVE":
            if zc.mechHandshake != nil:
              zc.state = ZmtpHandshake
              if not zc.asServer:
                discard zc.mechHandshake(zc, @[])
            else:
              if zc.onError != nil: zc.onError(zc, "CURVE not configured on this peer")
              zc.state = ZmtpClosed
              done = true
          else:
            if zc.onError != nil: zc.onError(zc, "Unsupported mechanism: " & mechanism)
            zc.state = ZmtpClosed
            done = true

    of ZmtpHandshake:
      let (ok, flags, body, bodyLen, consumed) = readFrame(zc, buf, zc.recvLen)
      if not ok:
        done = true
      elif (flags and ZmtpCommand) != 0:
        if body != nil and bodyLen > 0:
          if zc.mechHandshake != nil:
            discard zc.mechHandshake(zc, cast[ptr UncheckedArray[byte]](body).toOpenArray(0, bodyLen - 1))
        zc.consume(consumed)
      else:
        done = true

    of ZmtpReady:
      let (ok, flags, body, bodyLen, consumed) = readFrame(zc, buf, zc.recvLen)
      if not ok:
        done = true
      elif (flags and ZmtpCommand) == 0:
        done = true
      else:
        if zc.mechHandshake != nil:
          if body != nil and bodyLen > 0:
            discard zc.mechHandshake(zc, cast[ptr UncheckedArray[byte]](body).toOpenArray(0, bodyLen - 1))
        else:
          if body != nil and bodyLen > 0:
            handleCommand(zc, body, bodyLen)
        zc.consume(consumed)

    of ZmtpEstablished:
      let (ok, flags, body, bodyLen, consumed) = readFrame(zc, buf, zc.recvLen)
      if not ok:
        done = true
      else:
        establishedFrame(zc, flags, body, bodyLen, consumed, zc.consume(consumed))
        if zc.state == ZmtpClosed:
          done = true

    of ZmtpClosed:
      done = true
  # Trim recvBuf if it grew large and is now idle
  if zc.recvLen == 0 and zc.recvBuf.len > 8192:
    zc.recvBuf.setLen(4096)

# ── Connection setup ─────────────────────────────────────────────────────────

proc pendingBytes*(zc: ZmtpConnection): int =
  ## Bytes queued in powpow userspace write buffers for this connection
  ## (0 = fully drained). Backpressure signal covering ZMTP greeting,
  ## frames, TLS output and filetx fallback buffers.
  if zc == nil or zc.conn == nil: return 0
  zc.conn.pendingBytes()

proc sendScratchCap*(zc: ZmtpConnection): int =
  ## Capacity of the per-connection WS-framing scratch pool (bytes).
  ## Grows once to the largest frame tunneled, then stays flat.
  if zc == nil: return 0
  zc.sendScratch.cap()

proc sendScratchLen*(zc: ZmtpConnection): int =
  ## Used length of the per-connection WS-framing scratch pool.
  if zc == nil: return 0
  zc.sendScratch.len

proc flushWriteBuffer*(zc: ZmtpConnection): bool =
  ## Kicks the powpow write pump; returns true when fully drained.
  if zc == nil or zc.conn == nil: return true
  zc.conn.flushWriteBuffer()

proc initZmtpConnection*(conn: Connection; asServer: bool; mechanism: string = "NULL"; skipGreeting: bool = false): ZmtpConnection =
  result = ZmtpConnection(
    conn: conn,
    state: ZmtpGreeting,
    recvBuf: newSeq[byte](4096),
    mechanism: mechanism,
    asServer: asServer,
    recvOff: 0,
    sendScratch: initPool(byte, 4096),
  )
  if not skipGreeting:
    discard result.conn.send(buildGreeting(asServer, mechanism))
