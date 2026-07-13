## tests/test_auth.nim — Tests for PLAIN auth and ZAP callback.

import whiz/pair
import whiz/auth
import std/[unittest, os]

var nextPort = 25000
proc allocPort: int = result = nextPort; inc nextPort

suite "plain auth":

  test "test_plain_auth_valid_credentials":
    var established = false
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.setPlainAuth("admin", "secret")
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    cli.setPlainAuth("admin", "secret")
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      if cli.conn != nil and cli.conn.state == ZmtpEstablished:
        established = true
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check established
    cli.close()
    srv.close()
    loop.close()

  test "test_plain_auth_zap_callback":
    var authenticated = false
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.setPlainAuth("admin", "secret")
    srv.onAuthenticate(proc(creds: ZapCredentials): bool =
      authenticated = true
      creds.username == "admin" and creds.password == "secret")
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    cli.setPlainAuth("admin", "secret")
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(200) do (id: int):
      discard
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check authenticated
    cli.close()
    srv.close()
    loop.close()

  test "test_plain_auth_wrong_credentials":
    var gotError = false
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.setPlainAuth("admin", "secret")
    srv.onAuthenticate(proc(creds: ZapCredentials): bool =
      creds.username == "admin" and creds.password == "secret")
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    cli.setPlainAuth("admin", "wrongpass")
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      discard
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      if srv.conn != nil and srv.conn.state == ZmtpClosed:
        gotError = true
    discard loop.addTimer(500) do (id: int):
      loop.stop()
    loop.run()
    check gotError
    cli.close()
    srv.close()
    loop.close()
