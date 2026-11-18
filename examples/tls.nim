## examples/tls.nim — PAIR over TLS (ephemeral self-signed cert).
## Requires the openssl CLI to mint a throwaway cert (as in tests).
## Run: nim c -r examples/tls.nim  (POSIX only)
import whiz/pair
import std/os

const CertFile = "/tmp/whiz-example-cert.pem"
const KeyFile = "/tmp/whiz-example-key.pem"

let rc = execShellCmd("openssl req -x509 -newkey rsa:2048 -nodes " &
  "-keyout " & KeyFile & " -out " & CertFile & " -days 1 " &
  "-subj /CN=localhost 2>/dev/null")
doAssert rc == 0, "openssl cert generation failed"

proc toStr(data: openArray[byte]): string =
  result = newString(data.len)
  if data.len > 0: copyMem(addr result[0], unsafeAddr data[0], data.len)

let loop = newLoop()
let srv = newPairSocket(loop)
srv.setTlsServer(CertFile, KeyFile)
srv.setTlsAlpn(["zmtp/3.0"])
srv.setTlsKtlsAuto()
srv.`bind`("127.0.0.1", 5557)
let cli = newPairSocket(loop)
cli.setTlsClient(verifyPeer = false)
cli.setTlsAlpn(["zmtp/3.0"])

srv.onMessage = proc(data: openArray[byte]) =
  echo "server got over TLS: ", toStr(data)
  echo "pending bytes: ", srv.pendingBytes()
  loop.stop()

discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5557)
discard loop.addTimer(400) do (id: int):
  discard cli.send("hello tls")
discard loop.addTimer(2000) do (id: int):
  echo "(timeout)"; loop.stop()
loop.run()
cli.close(); srv.close(); loop.close()
try: removeFile(CertFile); removeFile(KeyFile)
except OSError: discard
