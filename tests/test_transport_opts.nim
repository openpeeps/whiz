## tests/test_transport_opts.nim — Tests for TLS kTLS/ALPN and WS transport options.
import whiz/pair
import whiz/ws
import std/[unittest, os]

const tlsCertFile = "/tmp/whiz-tls-opts-cert.pem"
const tlsKeyFile = "/tmp/whiz-tls-opts-key.pem"

let rc = execShellCmd("openssl req -x509 -newkey rsa:2048 -nodes " &
  "-keyout " & tlsKeyFile & " -out " & tlsCertFile & " -days 1 " &
  "-subj /CN=localhost 2>/dev/null")
doAssert rc == 0, "openssl cert generation failed"

var nextPort = 29200
proc allocPort: int = result = nextPort; inc nextPort

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

suite "transport opts":
  test "test_tls_alpn_and_ktls_helpers":
    # Pure API coverage: ALPN set, kTLS mode set, supported query.
    check ktlsSupported() == ktlsSupported()
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.setTlsServer(tlsCertFile, tlsKeyFile)
    srv.setTlsAlpn(["zmtp/3.0"])
    srv.setTlsKtlsAuto()
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    cli.setTlsClient(verifyPeer = false)
    cli.setTlsAlpn(["zmtp/3.0"])
    cli.setTlsKtlsAuto()
    var received = ""
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      received = toStr(data)
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      discard cli.send("hi")
    discard loop.addTimer(700) do (id: int):
      loop.stop()
    loop.run()
    check received == "hi"
    # Introspection helpers must not crash on live conns.
    if cli.conn != nil and cli.conn.conn != nil:
      discard cli.conn.conn.pendingBytes()
      discard cli.conn.flushWriteBuffer()
      discard cli.pendingBytes()
      discard tlsAlpnSelected(cli.conn.conn)
      discard isTlsActive(cli.conn.conn)
      discard ktlsActive(cli.conn.conn)
    cli.close()
    srv.close()
    loop.close()

  test "test_ws_opts_and_pending":
    var received = 0
    let loop = newLoop()
    let port = allocPort()
    let srv = newWsPairSocket(loop)
    srv.`bind`("127.0.0.1", port, opts = WsTransportOptions(
      permessageDeflate: false, pingIntervalMs: 50,
      maxFrameSize: DefaultMaxFrameSize, idleTimeoutMs: 0))
    let cli = newWsPairSocket(loop)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      received += 1
    discard loop.addTimer(100) do (id: int):
      cli.connect("127.0.0.1", port, opts = WsTransportOptions(
        permessageDeflate: false, pingIntervalMs: 50,
        maxFrameSize: DefaultMaxFrameSize, idleTimeoutMs: 0))
    discard loop.addTimer(500) do (id: int):
      discard cli.pendingBytes()
      discard cli.send("hello")
    discard loop.addTimer(900) do (id: int):
      loop.stop()
    loop.run()
    check received == 1
    cli.close()
    srv.close()
    loop.close()

try:
  removeFile(tlsCertFile)
  removeFile(tlsKeyFile)
except OSError:
  discard
