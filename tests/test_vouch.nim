## tests/test_vouch.nim — Tests for CURVE vouch (Ed25519 key continuity).
##
## HELLO carries vouchPub + sig(ephPub) when the client sets a vouch key.
## Servers verify the signature and optionally require a specific key.

import whiz/pair
import whiz/curve
import std/unittest

var nextPort = 27000
proc allocPort: int = result = nextPort; inc nextPort

suite "curve vouch":
  test "test_vouch_valid_passes":
    var established = false
    var msgOk = false
    let loop = newLoop()
    let port = allocPort()
    let (srvSec, srvPub) = generateCurveKeypair()
    let (cliSec, cliPub) = generateCurveKeypair()
    let (vouchSec, vouchPub) = generateVouchKeypair()

    let srv = newPairSocket(loop)
    srv.setCurveKeypair(srvPub, srvSec)
    srv.setCurveExpectedVouch(vouchPub)
    srv.`bind`("127.0.0.1", port)

    let cli = newPairSocket(loop)
    cli.setCurveClient(cliPub, cliSec, srvPub)
    cli.setCurveVouch(vouchPub, vouchSec)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      if data.len == 5 and data[0] == byte('h'): msgOk = true

    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(350) do (id: int):
      if cli.conn != nil and cli.conn.state == ZmtpEstablished and
         srv.conn != nil and srv.conn.state == ZmtpEstablished:
        established = true
        discard cli.send("hello")
    discard loop.addTimer(600) do (id: int):
      loop.stop()
    loop.run()
    check established
    check msgOk
    check CurveState(srv.conn.mechData).hasPeerVouch
    cli.close()
    srv.close()
    loop.close()

  test "test_vouch_missing_rejected_when_required":
    var srvEstablished = false
    let loop = newLoop()
    let port = allocPort()
    let (srvSec, srvPub) = generateCurveKeypair()
    let (cliSec, cliPub) = generateCurveKeypair()
    let (_, vouchPub) = generateVouchKeypair()

    let srv = newPairSocket(loop)
    srv.setCurveKeypair(srvPub, srvSec)
    srv.setCurveExpectedVouch(vouchPub)
    srv.`bind`("127.0.0.1", port)

    let cli = newPairSocket(loop)
    cli.setCurveClient(cliPub, cliSec, srvPub)
    # no vouch set on client

    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      if srv.conn != nil and srv.conn.state == ZmtpEstablished:
        srvEstablished = true
      loop.stop()
    loop.run()
    check not srvEstablished
    cli.close()
    srv.close()
    loop.close()

  test "test_vouch_wrong_key_rejected":
    var srvEstablished = false
    let loop = newLoop()
    let port = allocPort()
    let (srvSec, srvPub) = generateCurveKeypair()
    let (cliSec, cliPub) = generateCurveKeypair()
    let (_, expectedPub) = generateVouchKeypair()
    let (otherSec, otherPub) = generateVouchKeypair()

    let srv = newPairSocket(loop)
    srv.setCurveKeypair(srvPub, srvSec)
    srv.setCurveExpectedVouch(expectedPub)
    srv.`bind`("127.0.0.1", port)

    let cli = newPairSocket(loop)
    cli.setCurveClient(cliPub, cliSec, srvPub)
    cli.setCurveVouch(otherPub, otherSec)

    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(500) do (id: int):
      if srv.conn != nil and srv.conn.state == ZmtpEstablished:
        srvEstablished = true
      loop.stop()
    loop.run()
    check not srvEstablished
    cli.close()
    srv.close()
    loop.close()

  test "test_legacy_hello_passes_without_require":
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
    discard loop.addTimer(400) do (id: int):
      if cli.conn != nil and cli.conn.state == ZmtpEstablished and
         srv.conn != nil and srv.conn.state == ZmtpEstablished:
        established = true
      loop.stop()
    loop.run()
    check established
    cli.close()
    srv.close()
    loop.close()
