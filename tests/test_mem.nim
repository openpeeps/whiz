## tests/test_mem.nim — Memory-reuse regression tests (buffalo scratch).
##
## The WS framing path reuses a per-connection pool instead of allocating
## a seq per frame. A burst of mixed-size messages must arrive intact and
## in order even as the scratch grows and is reused.
import whiz/ws
import std/unittest

suite "mem":
  test "test_ws_burst_mixed_sizes":
    var received: seq[string] = @[]
    var expected: seq[string] = @[]
    for i in 0 ..< 200:
      # NOTE: empty frames are dropped on receive (feed requires bodyLen > 0),
      # so burst sizes start at 1.
      let size = case i mod 4
        of 0: 1
        of 1: 13
        of 2: 300 # forces long-frame header
        else: 5000
      var s = newString(size)
      for j in 0 ..< size:
        s[j] = char(byte((i + j) and 0xFF))
      expected.add(s)
    let loop = newLoop()
    let srv = newWsPairSocket(loop)
    srv.`bind`("127.0.0.1", 27401)
    let cli = newWsPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) =
      var s = newString(data.len)
      if data.len > 0:
        copyMem(addr s[0], unsafeAddr data[0], data.len)
      received.add(s)
      if received.len == expected.len:
        loop.stop()
    discard loop.addTimer(100) do (id: int):
      cli.connect("127.0.0.1", 27401)
    discard loop.addTimer(400) do (id: int):
      for s in expected:
        discard cli.send(s)
    discard loop.addTimer(5000) do (id: int):
      loop.stop()
    loop.run()
    check received.len == expected.len
    for i in 0 ..< expected.len:
      check received[i] == expected[i]
    cli.close()
    srv.close()
    loop.close()
