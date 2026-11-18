## examples/pair.nim — PAIR: exclusive 1-to-1 bidirectional messaging.
## Run: nim c -r examples/pair.nim
import whiz/pair

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let loop = newLoop()
let srv = newPairSocket(loop)
srv.`bind`("127.0.0.1", 5550)
let cli = newPairSocket(loop)

srv.onMessage = proc(data: openArray[byte]) =
  echo "server got: ", toStr(data)
  discard srv.send("hello from server")
cli.onMessage = proc(data: openArray[byte]) =
  echo "client got: ", toStr(data)
  loop.stop()

discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5550)
discard loop.addTimer(250) do (id: int):
  discard cli.send("hello from client")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
