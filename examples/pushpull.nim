## examples/pushpull.nim — PUSH/PULL pipeline, round-robin over workers.
## Run: nim c -r examples/pushpull.nim
import whiz/pushpull

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let loop = newLoop()
let push = newPushSocket(loop)
push.`bind`("127.0.0.1", 5553)
let pull = newPullSocket(loop)

var got = 0
pull.onMessage = proc(data: openArray[byte]) =
  inc got
  echo "worker got: ", toStr(data)
  if got >= 3: loop.stop()

discard loop.addTimer(50) do (id: int):
  pull.connect("127.0.0.1", 5553)
discard loop.addTimer(300) do (id: int):
  discard push.send("task 1")
  discard push.send("task 2")
  discard push.send("task 3")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout, got=", got, ")"; loop.stop()
loop.run()
push.close(); pull.close(); loop.close()
