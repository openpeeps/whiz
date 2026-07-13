## tests/test_filetx_receiver.nim — Standalone receiver stress test.
##
## Receiver binds first, sender connects after a delay (t=50). Tests both
## TCP and Unix domain socket transports. Focus is the receiver: sequential
## chunk writes, reqBuf reuse, progress callbacks, completion detection.

import whiz/pair
import whiz/filetx
import std/[unittest, os]

proc occupiedMem(): int64 =
  when defined(nimTypeNames):
    result = getTotalMem()
  else:
    result = 0

var nextPort = 30000
proc allocPort: int = result = nextPort; inc nextPort

const TmpDir = "/tmp/powpow_test_filetx_receiver"

proc writeTestFile(path: string; size: int) =
  var f = open(path, fmWrite)
  for i in 0 ..< size:
    f.write(byte((i and 0x7F) + 32).char)
  f.close()

suite "filetx receiver":

  test "receiver_tcp":
    let loop = newLoop()
    let port = allocPort()
    let srcPath = TmpDir / "src_tcp.bin"
    let dstPath = TmpDir / "dst_tcp.bin"
    createDir(TmpDir)
    writeTestFile(srcPath, 10_000_000)

    let srv = newPairSocket(loop)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)

    var sender: FileSender
    var receiver: FileReceiver
    var recvProgress: int64 = 0
    var recvComplete = false

    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)

    discard loop.addTimer(200) do (id: int):
      if cli.conn == nil: return
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
          receiver.onProgress = proc(received, total: int64) {.closure.} =
            recvProgress = received
          receiver.onComplete = proc(path: string) {.closure.} =
            recvComplete = true
        discard receiver.feed(data)
      sender.start()

    discard loop.addTimer(10000) do (id: int):
      loop.stop()

    loop.run()
    check fileExists(dstPath)
    check getFileSize(srcPath) == getFileSize(dstPath)
    check recvProgress == 10_000_000
    check recvComplete
    removeFile(srcPath); removeFile(dstPath)
    cli.close(); srv.close(); loop.close()

  when not defined(windows):
    test "receiver_unix":
      let loop = newLoop()
      let socketPath = TmpDir / "receiver.sock"
      removeFile(socketPath)
      let srcPath = TmpDir / "src_unix.bin"
      let dstPath = TmpDir / "dst_unix.bin"
      createDir(TmpDir)
      writeTestFile(srcPath, 10_000_000)

      let srv = newPairSocket(loop)
      srv.`bind`(socketPath, transport = TransportUnix)
      let cli = newPairSocket(loop)

      var sender: FileSender
      var receiver: FileReceiver
      var recvProgress: int64 = 0
      var recvComplete = false

      discard loop.addTimer(50) do (id: int):
        cli.connect(socketPath, transport = TransportUnix)

      discard loop.addTimer(200) do (id: int):
        if cli.conn == nil: return
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
            receiver.onProgress = proc(received, total: int64) {.closure.} =
              recvProgress = received
            receiver.onComplete = proc(path: string) {.closure.} =
              recvComplete = true
          discard receiver.feed(data)
        sender.start()

      discard loop.addTimer(10000) do (id: int):
        loop.stop()

      loop.run()
      check fileExists(dstPath)
      check getFileSize(srcPath) == getFileSize(dstPath)
      check recvProgress == 10_000_000
      check recvComplete
      removeFile(srcPath); removeFile(dstPath)
      removeFile(socketPath)
      cli.close(); srv.close(); loop.close()
