## examples/filetx.nim — File transfer over a PAIR connection.
## Sends /tmp/whiz-example-src.bin -> /tmp/whiz-example-dst.bin.
## Run: nim c -r examples/filetx.nim
import whiz/pair
import whiz/filetx
import std/os

const SrcPath = "/tmp/whiz-example-src.bin"
const DstPath = "/tmp/whiz-example-dst.bin"

block: # make a small source file
  var f = open(SrcPath, fmWrite)
  for i in 0 ..< 20_000:
    f.write(byte((i and 0x7F) + 32).char)
  f.close()

let loop = newLoop()
let srv = newPairSocket(loop)
srv.`bind`("127.0.0.1", 5559)
let cli = newPairSocket(loop)

var receiver: FileReceiver
var sender: FileSender

discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5559)
discard loop.addTimer(250) do (id: int):
  sender = newFileSender(cli.conn, SrcPath)
  sender.onProgress = proc(sent, total: int64) =
    if sent mod 8192 == 0: echo "sent ", sent, "/", total
  sender.onComplete = proc() = echo "sender done"
  cli.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
    if receiver != nil:
      if not receiver.feed(data): sender.feedReq(data)
    else: sender.feedReq(data)
  srv.conn.onMessage = proc(zc: ZmtpConnection; data: openArray[byte]) =
    if receiver == nil:
      receiver = newFileReceiver(srv.conn, DstPath)
      receiver.onProgress = proc(got, total: int64) =
        if got mod 8192 == 0: echo "received ", got, "/", total
      receiver.onComplete = proc(path: string) =
        echo "saved to ", path, " (", getFileSize(path), " bytes)"
        loop.stop()
    discard receiver.feed(data)
  sender.start()
discard loop.addTimer(5000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
