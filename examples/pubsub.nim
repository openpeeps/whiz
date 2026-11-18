## examples/pubsub.nim — PUB/SUB with topic filtering.
## Run: nim c -r examples/pubsub.nim
import whiz/pubsub

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let loop = newLoop()
let pub = newPubSocket(loop, "127.0.0.1", 5551)
let sub = newSubSocket(loop)
sub.onMessage = proc(topic, data: openArray[byte]) =
  echo "sub got [", toStr(topic), "]: ", toStr(data)
  loop.stop()
sub.subscribe("sports.")

discard loop.addTimer(100) do (id: int):
  sub.connect("127.0.0.1", 5551)
discard loop.addTimer(400) do (id: int):
  pub.publish("news.breaking") # filtered out: no matching subscriber
  pub.publish("sports.soccer: goal!")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
sub.close(); pub.close(); loop.close()
