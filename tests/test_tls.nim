## tests/test_tls.nim — Tests for TLS transport (TCP only, POSIX only).
##
## Certificates are ephemeral and self-signed: generated with the openssl
## CLI at startup, clients connect with verifyPeer=false.

import whiz/pair
import whiz/clientserver
import std/[unittest, os]

const tlsCertFile = "/tmp/whiz-tls-test-cert.pem"
const tlsKeyFile = "/tmp/whiz-tls-test-key.pem"

let rc = execShellCmd("openssl req -x509 -newkey rsa:2048 -nodes " &
  "-keyout " & tlsKeyFile & " -out " & tlsCertFile & " -days 1 " &
  "-subj /CN=localhost 2>/dev/null")
doAssert rc == 0, "openssl cert generation failed"

var nextPort = 29100
proc allocPort: int = result = nextPort; inc nextPort

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

suite "tls":

  test "test_tls_pair_basic":
    var received = ""
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.setTlsServer(tlsCertFile, tlsKeyFile)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    cli.setTlsClient(verifyPeer = false)
    srv.onMessage = proc(data: openArray[byte]) {.closure.} =
      received = toStr(data)
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      discard cli.send("hello tls")
    discard loop.addTimer(700) do (id: int):
      loop.stop()
    loop.run()
    check received == "hello tls"
    cli.close()
    srv.close()
    loop.close()

  test "test_tls_clientserver":
    var replied = ""
    let loop = newLoop()
    let port = allocPort()
    let srv = newServerSocket(loop)
    srv.setTlsServer(tlsCertFile, tlsKeyFile)
    srv.`bind`("127.0.0.1", port)
    let cli = newClientSocket(loop)
    cli.setTlsClient(verifyPeer = false)
    srv.onRequest = proc(client: ServerClient; data: openArray[byte]) {.closure.} =
      discard srv.sendTo(client, "tls:" & toStr(data))
    cli.onReply = proc(data: openArray[byte]) {.closure.} =
      replied = toStr(data)
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(300) do (id: int):
      discard cli.send("hello")
    discard loop.addTimer(700) do (id: int):
      loop.stop()
    loop.run()
    check replied == "tls:hello"
    cli.close()
    srv.close()
    loop.close()

  test "test_tls_plaintext_client_rejected":
    ## A plaintext client against a TLS server never establishes ZMTP.
    let loop = newLoop()
    let port = allocPort()
    let srv = newPairSocket(loop)
    srv.setTlsServer(tlsCertFile, tlsKeyFile)
    srv.`bind`("127.0.0.1", port)
    let cli = newPairSocket(loop)
    discard loop.addTimer(50) do (id: int):
      cli.connect("127.0.0.1", port)
    discard loop.addTimer(600) do (id: int):
      loop.stop()
    loop.run()
    check cli.conn == nil or cli.conn.state != ZmtpEstablished
    cli.close()
    srv.close()
    loop.close()

try:
  removeFile(tlsCertFile)
  removeFile(tlsKeyFile)
except OSError:
  discard
