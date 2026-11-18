## tests/test_ownloop.nim — Tests for loop-less socket constructors.
import whiz/pair
import whiz/reqrep
import whiz/ws
import std/unittest

suite "ownloop":
  test "test_pair_ownloop":
    var received = 0
    let srv = newPairSocket() # owns its loop
    check srv.getLoop() != nil
    srv.`bind`("127.0.0.1", 27301)
    let cli = newPairSocket(srv.getLoop()) # borrow server loop
    srv.onMessage = proc(data: openArray[byte]) = inc received
    discard srv.getLoop().addTimer(50) do (id: int):
      cli.connect("127.0.0.1", 27301)
    discard srv.getLoop().addTimer(200) do (id: int):
      discard cli.send("hi")
    discard srv.getLoop().addTimer(500) do (id: int):
      srv.stop()
    srv.run()
    check received == 1
    cli.close()
    srv.close() # frees the owned loop

  test "test_reqrep_ownloop":
    var reply = ""
    let rep = newRepSocket()
    rep.`bind`("127.0.0.1", 27302)
    let req = newReqSocket(rep.getLoop())
    rep.onRequest = proc(data: openArray[byte]) =
      discard rep.send("ok")
    req.onReply = proc(data: openArray[byte]) =
      reply = "ok"
      req.stop()
    discard rep.getLoop().addTimer(50) do (id: int):
      req.connect("127.0.0.1", 27302)
    discard rep.getLoop().addTimer(250) do (id: int):
      discard req.send("hi")
    discard rep.getLoop().addTimer(1500) do (id: int):
      rep.stop()
    rep.run()
    check reply == "ok"
    req.close()
    rep.close()

  test "test_wspair_ownloop":
    var received = 0
    let srv = newWsPairSocket()
    srv.`bind`("127.0.0.1", 27303)
    let cli = newWsPairSocket(srv.getLoop())
    srv.onMessage = proc(data: openArray[byte]) =
      inc received
      srv.stop()
    discard srv.getLoop().addTimer(100) do (id: int):
      cli.connect("127.0.0.1", 27303)
    discard srv.getLoop().addTimer(400) do (id: int):
      discard cli.send("hello")
    discard srv.getLoop().addTimer(1500) do (id: int):
      srv.stop()
    srv.run()
    check received == 1
    cli.close()
    srv.close()
