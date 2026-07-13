## tests/test_filetx.nim — Tests for file transfer over ZMTP.

import whiz/pair
import whiz/filetx
import std/[unittest, os]

var nextPort = 27000
proc allocPort: int = result = nextPort; inc nextPort

const TmpDir = "/tmp/powpow_test_filetx"

proc writeTestFile(path: string; size: int) =
  var f = open(path, fmWrite)
  for i in 0 ..< size:
    f.write(byte((i and 0x7F) + 32).char)
  f.close()

proc readAll(path: string): string =
  result = readFile(path)

suite "filetx":

  test "test_filetx_small":
    let loop = newLoop()
    let port = allocPort()
    let srcPath = TmpDir / "src_small.bin"
    let dstPath = TmpDir / "dst_small.bin"
    createDir(TmpDir)
    writeTestFile(srcPath, 4096)

    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    var receiver: FileReceiver
    var sender: FileSender
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      sender = newFileSender(cli.conn, srcPath)
      cli.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        if receiver != nil:
          if not receiver.feed(data):
            sender.feedReq(data)
        else:
          sender.feedReq(data)
      srv.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        if receiver == nil:
          receiver = newFileReceiver(srv.conn, dstPath)
        discard receiver.feed(data)
      sender.start()
    discard loop.addTimer(1000) do (id: int):
      loop.stop()
    loop.run()
    check fileExists(dstPath)
    check readAll(srcPath) == readAll(dstPath)
    removeFile(srcPath); removeFile(dstPath)
    cli.close(); srv.close(); loop.close()

  test "test_filetx_large":
    let loop = newLoop()
    let port = allocPort()
    let srcPath = TmpDir / "src_large.bin"
    let dstPath = TmpDir / "dst_large.bin"
    createDir(TmpDir)
    writeTestFile(srcPath, 200_000)

    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    var receiver: FileReceiver
    var sender: FileSender
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      sender = newFileSender(cli.conn, srcPath)
      cli.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        if receiver != nil:
          if not receiver.feed(data):
            sender.feedReq(data)
        else:
          sender.feedReq(data)
      srv.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        if receiver == nil:
          receiver = newFileReceiver(srv.conn, dstPath)
        discard receiver.feed(data)
      sender.start()
    discard loop.addTimer(3000) do (id: int):
      loop.stop()
    loop.run()
    check fileExists(dstPath)
    check readAll(srcPath) == readAll(dstPath)
    removeFile(srcPath); removeFile(dstPath)
    cli.close(); srv.close(); loop.close()

  test "test_filetx_cancel":
    let loop = newLoop()
    let port = allocPort()
    let srcPath = TmpDir / "src_cancel.bin"
    let dstPath = TmpDir / "dst_cancel.bin"
    createDir(TmpDir)
    removeFile(dstPath)  # ensure clean state
    writeTestFile(srcPath, 100_000)

    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    var errMsg = ""
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      let sender = newFileSender(cli.conn, srcPath)
      sender.onError = proc(reason: string) {.closure.} = errMsg = reason
      srv.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) = discard
      cli.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
        sender.feedReq(data)
      sender.cancel("cancelled before start")
      sender.start()
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check not fileExists(dstPath)
    removeFile(srcPath)
    cli.close(); srv.close(); loop.close()

  test "test_filetx_missing_src":
    let loop = newLoop()
    let port = allocPort()
    let srcPath = TmpDir / "nonexistent.bin"
    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    var errMsg = ""
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      let sender = newFileSender(cli.conn, srcPath)
      sender.onError = proc(reason: string) {.closure.} = errMsg = reason
      sender.start()
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check errMsg.len > 0
    cli.close(); srv.close(); loop.close()
