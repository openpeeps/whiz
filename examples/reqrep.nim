## examples/reqrep.nim — REQ/REP strict request-reply.
## Run: nim c -r examples/reqrep.nim
import whiz/reqrep

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let loop = newLoop()
let rep = newRepSocket(loop)
rep.`bind`("127.0.0.1", 5552)
let req = newReqSocket(loop)

rep.onRequest = proc(data: openArray[byte]) =
  echo "server got: ", toStr(data)
  discard rep.send("reply: " & toStr(data))
req.onReply = proc(data: openArray[byte]) =
  echo "client got: ", toStr(data)
  loop.stop()

discard loop.addTimer(50) do (id: int):
  req.connect("127.0.0.1", 5552)
discard loop.addTimer(250) do (id: int):
  discard req.send("hello")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
req.close(); rep.close(); loop.close()
