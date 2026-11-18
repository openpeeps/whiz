## examples/curve.nim — CURVE encrypted PAIR (X25519 + XChaCha20-Poly1305).
## Run: nim c -r examples/curve.nim
import whiz/pair
import whiz/curve

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let (srvSec, srvPub) = generateCurveKeypair()
let (cliSec, cliPub) = generateCurveKeypair()

let loop = newLoop()
let srv = newPairSocket(loop)
srv.setCurveKeypair(srvPub, srvSec)
srv.`bind`("127.0.0.1", 5555)
let cli = newPairSocket(loop)
cli.setCurveClient(cliPub, cliSec, srvPub)

srv.onMessage = proc(data: openArray[byte]) =
  echo "server got (decrypted): ", toStr(data)
  loop.stop()

discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5555)
discard loop.addTimer(400) do (id: int):
  discard cli.send("secret hello")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
