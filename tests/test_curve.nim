## tests/test_curve.nim — Tests for CURVE auth and encrypted communication.

import whiz/pair
import whiz/curve
import std/[unittest, os]

var nextPort = 26000
proc allocPort: int = result = nextPort; inc nextPort

suite "curve auth":

  test "test_curve_handshake_and_encryption":
    var srvReady = false
    var cliReady = false
    var msgReceived = false
    let loop = newLoop()
    let port = allocPort()
    let (srvSec, srvPub) = generateCurveKeypair()
    let (cliSec, cliPub) = generateCurveKeypair()

    let srv = newPairSocket(loop)
    srv.setCurveKeypair(srvPub, srvSec)
    srv.`bind`("127.0.0.1", port)

    let cli = newPairSocket(loop)
    cli.setCurveClient(cliPub, cliSec, srvPub)

    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      msgReceived = true

    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)

    discard loop.addTimer(300) do (id: int):
      if cli.conn != nil and cli.conn.state == ZmtpEstablished:
        cliReady = true
      if srv.conn != nil and srv.conn.state == ZmtpEstablished:
        srvReady = true
      if cliReady and srvReady:
        cli.send("hello encrypted")

    discard loop.addTimer(500) do (id: int):
      loop.stop()

    loop.run()
    check srvReady
    check cliReady
    check msgReceived
    cli.close()
    srv.close()
    loop.close()

  test "test_curve_auto_keypair":
    var established = false
    let loop = newLoop()
    let port = allocPort()
    let (srvSec, srvPub) = generateCurveKeypair()
    let (cliSec, cliPub) = generateCurveKeypair()

    let srv = newPairSocket(loop)
    srv.setCurveKeypair(srvPub, srvSec)
    srv.`bind`("127.0.0.1", port)

    let cli = newPairSocket(loop)
    cli.setCurveClient(cliPub, cliSec, srvPub)

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
