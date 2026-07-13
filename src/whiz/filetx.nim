## Chunked, flow-controlled file transfer over ZMTP.
##
## Receiver-driven pull flow control: sender sends FILE_INIT metadata,
## then waits for FILE_REQ from the receiver before sending each
## FILE_CHUNK. This provides natural back-pressure. Supports cancel
## from either side and restart from any offset.
##
## Usage (unicast over PAIR):
##
##   # Sender
##   let sender = newFileSender(cli.conn, "/path/to/file")
##   var fr = newFileReceiver(srv.conn, "/tmp/out")
##   cli.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
##     if not fr.feed(data): sender.feedReq(data)
##   sender.start()

import std/os
when not defined(windows):
  proc c_read(fd: cint; buf: pointer; count: csize_t): cint {.importc: "read", header: "<unistd.h>".}
import powpow/[loop, types, net/tcp]
import powpow/net/common
import ./zmtp

export loop, types, tcp, zmtp

const
  FileTxChunkSize* = 65536

  FTypeInit*   = 0x01
  FTypeChunk*  = 0x02
  FTypeReq*    = 0x03
  FTypeCancel* = 0x04
  FTypeDone*   = 0x05

type
  FileSender* = ref object
    conn*:      ZmtpConnection
    path:       string
    fd:         int         # POSIX fd from openFileRead
    fileSize:   int64
    offset:     int64
    chunkSize:  int
    cancelled:  bool
    onProgress*: proc(sent, total: int64) {.closure.}
    onComplete*: proc() {.closure.}
    onError*:    proc(reason: string) {.closure.}

  FileReceiver* = ref object
    conn:       ZmtpConnection
    path:       string
    fp:         File        # Nim File for writing
    fileSize:   int64
    offset:     int64
    chunkSize:  int
    cancelled:  bool
    onProgress*: proc(received, total: int64) {.closure.}
    onComplete*: proc(path: string) {.closure.}
    onError*:    proc(reason: string) {.closure.}

# ── Helpers ──────────────────────────────────────────────────────────────────

proc readI64(data: ptr UncheckedArray[byte]; pos: int): int64 =
  for i in 0 ..< 8: result = (result shl 8) or data[pos + i].int64

proc writeI64(data: ptr UncheckedArray[byte]; pos: int; val: int64) =
  var v = val
  for i in 0 ..< 8:
    data[pos + 7 - i] = byte(v and 0xFF); v = v shr 8

# ── FileSender ───────────────────────────────────────────────────────────────

proc newFileSender*(conn: ZmtpConnection; path: string;
                    chunkSize: int = FileTxChunkSize): FileSender =
  FileSender(conn: conn, path: path, fd: -1, chunkSize: chunkSize)

proc cancel*(fs: FileSender; reason: string = "") =
  if fs.cancelled: return
  fs.cancelled = true
  if fs.fd >= 0: closeFile(fs.fd); fs.fd = -1
  var body = newSeq[byte](1 + reason.len)
  body[0] = FTypeCancel
  if reason.len > 0: copyMem(addr body[1], addr reason[0], reason.len)
  fs.conn.sendMessage(body)

proc start*(fs: FileSender) =
  if fs.cancelled: return
  fs.fd = openFileRead(fs.path)
  if fs.fd < 0:
    if fs.onError != nil: fs.onError("Cannot open: " & fs.path)
    return
  fs.fileSize = getFileSize(fs.fd)
  if fs.fileSize < 0:
    closeFile(fs.fd); fs.fd = -1
    if fs.onError != nil: fs.onError("Cannot stat: " & fs.path)
    return
  fs.offset = 0
  let name = fs.path.extractFilename
  var body = newSeq[byte](1 + name.len + 1 + 8)
  body[0] = FTypeInit
  copyMem(addr body[1], addr name[0], name.len)
  body[1 + name.len] = 0
  writeI64(cast[ptr UncheckedArray[byte]](addr body[2 + name.len]), 0, fs.fileSize)
  fs.conn.sendMessage(body)

proc feedReq*(fs: FileSender; data: openArray[byte]) =
  if fs.cancelled or fs.fd < 0: return
  if data.len < 1: return
  if data[0] == FTypeCancel:
    fs.cancelled = true
    if fs.fd >= 0: closeFile(fs.fd); fs.fd = -1
    if fs.onError != nil: fs.onError("transfer cancelled by receiver")
    return
  if data.len < 9 or data[0] != FTypeReq: return
  let reqOff = readI64(cast[ptr UncheckedArray[byte]](unsafeAddr data[1]), 0)
  if reqOff != fs.offset: return
  let remaining = fs.fileSize - fs.offset
  if remaining <= 0: return
  let toSend = min(remaining, fs.chunkSize.int64).int
  var buf = newSeq[byte](1 + 8 + toSend)
  buf[0] = FTypeChunk
  writeI64(cast[ptr UncheckedArray[byte]](addr buf[1]), 0, fs.offset)
  if toSend > 0:
    let n = c_read(cint(fs.fd), cast[pointer](addr buf[9]), csize_t(toSend))
    if n <= 0:
      if fs.onError != nil: fs.onError("Read error at " & $fs.offset)
      return
    buf.setLen(1 + 8 + n)
  fs.conn.sendMessage(buf)
  fs.offset += (buf.len - 9)
  if fs.onProgress != nil: fs.onProgress(fs.offset, fs.fileSize)
  if fs.offset >= fs.fileSize:
    closeFile(fs.fd); fs.fd = -1
    var done = newSeq[byte](1); done[0] = FTypeDone
    fs.conn.sendMessage(done)
    if fs.onComplete != nil: fs.onComplete()

# ── FileReceiver ─────────────────────────────────────────────────────────────

proc newFileReceiver*(conn: ZmtpConnection; path: string;
                      chunkSize: int = FileTxChunkSize): FileReceiver =
  FileReceiver(conn: conn, path: path, chunkSize: chunkSize)

proc cancel*(fr: FileReceiver; reason: string = "") =
  if fr.cancelled: return
  fr.cancelled = true
  if fr.fp != nil:
    fr.fp.close()
    fr.fp = nil
    removeFile(fr.path)
  var body = newSeq[byte](1 + reason.len)
  body[0] = FTypeCancel
  if reason.len > 0: copyMem(addr body[1], addr reason[0], reason.len)
  fr.conn.sendMessage(body)

proc feed*(fr: FileReceiver; data: openArray[byte]): bool =
  if fr.cancelled or data.len < 1: return false
  let d = cast[ptr UncheckedArray[byte]](unsafeAddr data[0])
  case d[0]
  of FTypeInit:
    var nameLen = 0
    while 1 + nameLen < data.len and d[1 + nameLen] != 0: inc nameLen
    if nameLen > 0:
      let fn = newString(nameLen)
      copyMem(addr fn[0], addr d[1], nameLen)
      if fr.path.len == 0: fr.path = fn
    let sizePos = 2 + nameLen
    if sizePos + 8 <= data.len: fr.fileSize = readI64(d, sizePos)
    if fr.fp == nil:
      try: fr.fp = open(fr.path, fmWrite)
      except: discard
    if fr.fp == nil:
      if fr.onError != nil: fr.onError("Cannot create: " & fr.path)
      return false
    fr.offset = 0
    var req = newSeq[byte](9); req[0] = FTypeReq
    writeI64(cast[ptr UncheckedArray[byte]](addr req[1]), 0, 0)
    fr.conn.sendMessage(req)
    return true

  of FTypeChunk:
    if fr.cancelled or fr.fp == nil or data.len < 9: return false
    let chunkOff = readI64(d, 1)
    if chunkOff != fr.offset: return false
    let chunkLen = data.len - 9
    if chunkLen > 0:
      var buf = cast[ptr UncheckedArray[byte]](unsafeAddr d[9])
      let written = fr.fp.writeBuffer(buf, chunkLen)
      if written != chunkLen:
        if fr.onError != nil: fr.onError("Write error at " & $fr.offset)
        return false
    fr.offset += chunkLen
    if fr.onProgress != nil: fr.onProgress(fr.offset, fr.fileSize)
    if fr.offset < fr.fileSize:
      var req = newSeq[byte](9); req[0] = FTypeReq
      writeI64(cast[ptr UncheckedArray[byte]](addr req[1]), 0, fr.offset)
      fr.conn.sendMessage(req)
    return true

  of FTypeDone:
    if fr.fp != nil: fr.fp.close(); fr.fp = nil
    if fr.onComplete != nil: fr.onComplete(fr.path)
    return true

  of FTypeCancel:
    let reason = if data.len > 1:
                   var s = newString(data.len - 1)
                   copyMem(addr s[0], unsafeAddr data[1], data.len - 1)
                   s
                 else: "cancelled"
    if fr.fp != nil: fr.fp.close(); fr.fp = nil; removeFile(fr.path)
    if fr.onError != nil: fr.onError(reason)
    return true

  else: return false
