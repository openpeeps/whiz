## tests/test_feed.nim — Unit tests for ZmtpConnection.feed fast/staging paths.
##
## Constructs connections directly (no sockets) to drive feed with
## exact chunk boundaries: whole frames (fast path), split frames
## (staging path), multiple frames per chunk, long frames, and commands.
import whiz/zmtp
import std/unittest

proc mkConn(): ZmtpConnection =
  ZmtpConnection(state: ZmtpEstablished, socketType: "PAIR")

proc frameOf(payload: string): seq[byte] =
  ## Builds one short ZMTP data frame for payload.
  result = newSeq[byte](2 + payload.len)
  result[0] = 0
  result[1] = byte(payload.len)
  for i, c in payload: result[2 + i] = byte(c)

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

suite "feed":
  test "test_feed_whole_frame_fast_path":
    var got: seq[string] = @[]
    let zc = mkConn()
    zc.onMessage = proc(z: ZmtpConnection; data: openArray[byte]) =
      got.add(toStr(data))
    zc.feed(frameOf("hello"))
    check got == @["hello"]

  test "test_feed_split_frame_staging":
    var got: seq[string] = @[]
    let zc = mkConn()
    zc.onMessage = proc(z: ZmtpConnection; data: openArray[byte]) =
      got.add(toStr(data))
    let f = frameOf("split-me")
    zc.feed(f.toOpenArray(0, 3)) # partial header+body
    check got.len == 0
    zc.feed(f.toOpenArray(4, f.high)) # remainder
    check got == @["split-me"]

  test "test_feed_multi_frame_one_chunk":
    var got: seq[string] = @[]
    let zc = mkConn()
    zc.onMessage = proc(z: ZmtpConnection; data: openArray[byte]) =
      got.add(toStr(data))
    var chunk: seq[byte] = @[]
    for w in ["one", "two", "three"]:
      chunk.add(frameOf(w))
    zc.feed(chunk)
    check got == @["one", "two", "three"]

  test "test_feed_long_frame_split":
    var got: seq[string] = @[]
    let zc = mkConn()
    zc.onMessage = proc(z: ZmtpConnection; data: openArray[byte]) =
      got.add(toStr(data))
    var payload = newString(70000)
    for i in 0 ..< payload.len: payload[i] = char(byte(i and 0xFF))
    var f = newSeq[byte](10 + payload.len)
    f[0] = 0x02 # ZmtpLong
    let blen = uint64(payload.len)
    for i in 0 ..< 8:
      f[1 + i] = byte((blen shr ((7 - i) * 8)) and 0xFF)
    for i in 0 ..< payload.len: f[10 + i] = byte(payload[i])
    # split header, middle, tail across three feeds
    zc.feed(f.toOpenArray(0, 5))
    check got.len == 0
    zc.feed(f.toOpenArray(6, 40000))
    check got.len == 0
    zc.feed(f.toOpenArray(40001, f.high))
    check got.len == 1
    check got[0] == payload

  test "test_feed_partial_then_whole":
    var got: seq[string] = @[]
    let zc = mkConn()
    zc.onMessage = proc(z: ZmtpConnection; data: openArray[byte]) =
      got.add(toStr(data))
    let a = frameOf("first")
    let b = frameOf("second")
    # first frame split: head now, tail + whole second frame later
    zc.feed(a.toOpenArray(0, 2))
    var both = newSeq[byte](a.len - 3 + b.len)
    for i in 3 ..< a.len: both[i - 3] = a[i]
    for i in 0 ..< b.len: both[a.len - 3 + i] = b[i]
    zc.feed(both)
    check got == @["first", "second"]

  test "test_feed_subscribe_command":
    var topics: seq[string] = @[]
    let zc = mkConn()
    zc.onSubscribe = proc(z: ZmtpConnection; topic: openArray[byte]) =
      topics.add(toStr(topic))
    # SUBSCRIBE command: flags=cmd, body="SUBSCRIBE\0" + [1, topic...]
    var body = newSeq[byte]()
    for c in "SUBSCRIBE": body.add(byte(c))
    body.add(0)
    body.add(1)
    for c in "news": body.add(byte(c))
    var f = newSeq[byte](2 + body.len)
    f[0] = 0x04
    f[1] = byte(body.len)
    for i, b in body: f[2 + i] = b
    zc.feed(f)
    check topics == @["news"]

  test "test_feed_multipart":
    var got: seq[seq[byte]] = @[]
    let zc = mkConn()
    zc.onMultipart = proc(z: ZmtpConnection; frames: seq[seq[byte]]) =
      got = frames
    proc mframe(payload: string; more: bool): seq[byte] =
      result = newSeq[byte](2 + payload.len)
      result[0] = if more: 0x01.byte else: 0.byte
      result[1] = byte(payload.len)
      for i, c in payload: result[2 + i] = byte(c)
    var chunk = mframe("a", true) & mframe("b", true) & mframe("c", false)
    zc.feed(chunk)
    check got.len == 3
    check toStr(got[0]) == "a"
    check toStr(got[2]) == "c"
