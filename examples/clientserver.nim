## examples/clientserver.nim — CLIENT/SERVER with routing handles.
## Run: nim c -r examples/clientserver.nim
import whiz/clientserver

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let loop = newLoop()
let srv = newServerSocket(loop)
srv.`bind`("127.0.0.1", 5554)
let cli = newClientSocket(loop)

srv.onRequest = proc(client: ServerClient; data: openArray[byte]) =
  echo "server got from #", client.id, ": ", toStr(data)
  discard srv.sendTo(client, "hi client #" & $client.id)
cli.onReply = proc(data: openArray[byte]) =
  echo "client got: ", toStr(data)
  loop.stop()

discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5554)
discard loop.addTimer(250) do (id: int):
  discard cli.send("hello server")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
