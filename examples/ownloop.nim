## examples/ownloop.nim — Sockets that own their event loop (no manual newLoop).
## The server creates the loop; the client borrows it via getLoop().
## Run: nim c -r examples/ownloop.nim
import whiz/pair

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let srv = newPairSocket() # owns its loop
srv.`bind`("127.0.0.1", 5560)
let cli = newPairSocket(srv.getLoop()) # borrow the server's loop

srv.onMessage = proc(data: openArray[byte]) =
  echo "server got: ", toStr(data)
  discard srv.send("hello from server")
cli.onMessage = proc(data: openArray[byte]) =
  echo "client got: ", toStr(data)
  srv.stop()

discard srv.getLoop().addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5560)
discard srv.getLoop().addTimer(250) do (id: int):
  discard cli.send("hello from client")
discard srv.getLoop().addTimer(2000) do (id: int):
  echo "(timeout)"; srv.stop()
srv.run() # drives both sockets; srv.close() frees the loop below
cli.close()
srv.close()
