## examples/ws_pair.nim — PAIR over WebSocket with transport options.
## Run: nim c -r examples/ws_pair.nim
import whiz/ws

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let loop = newLoop()
let srv = newWsPairSocket(loop)
srv.`bind`("127.0.0.1", 5558, opts = WsTransportOptions(
  permessageDeflate: false, pingIntervalMs: 1000,
  maxFrameSize: DefaultMaxFrameSize, idleTimeoutMs: 0))
let cli = newWsPairSocket(loop)

srv.onMessage = proc(data: openArray[byte]) =
  echo "server got over WS: ", toStr(data)
  loop.stop()

discard loop.addTimer(100) do (id: int):
  cli.connect("127.0.0.1", 5558, opts = WsTransportOptions(
    permessageDeflate: false, pingIntervalMs: 1000,
    maxFrameSize: DefaultMaxFrameSize, idleTimeoutMs: 0))
discard loop.addTimer(500) do (id: int):
  discard cli.send("hello ws")
  echo "client pending bytes: ", cli.pendingBytes()
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
