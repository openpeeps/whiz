## examples/curve_vouch.nim — CURVE + Ed25519 vouch (key continuity).
## The server only accepts clients whose HELLO carries a valid vouch.
## Run: nim c -r examples/curve_vouch.nim
import whiz/pair
import whiz/curve

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let (srvSec, srvPub) = generateCurveKeypair()
let (cliSec, cliPub) = generateCurveKeypair()
let (vouchSec, vouchPub) = generateVouchKeypair()

let loop = newLoop()
let srv = newPairSocket(loop)
srv.setCurveKeypair(srvPub, srvSec)
srv.setCurveExpectedVouch(vouchPub)
srv.`bind`("127.0.0.1", 5556)
let cli = newPairSocket(loop)
cli.setCurveClient(cliPub, cliSec, srvPub)
cli.setCurveVouch(vouchPub, vouchSec)

srv.onMessage = proc(data: openArray[byte]) =
  echo "server got (vouched): ", toStr(data)
  loop.stop()

discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5556)
discard loop.addTimer(400) do (id: int):
  discard cli.send("vouched hello")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
