# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## CURVE security mechanism for ZMTP.
##
## Implements the ZMTP CURVE security mechanism using nimcypher's
## (pure-Nim Monocypher port) X25519 key exchange and
## XChaCha20-Poly1305 AEAD encryption.

import std/[sequtils]
import nimcypher
import ./zmtp

export zmtp

type
  CurvePhase* = enum
    cpInit
    cpWaitWelcome
    cpWaitInitiate
    cpWaitReady

  CurveState* = ref object of RootObj
    phase*: CurvePhase
    isServer*: bool
    permSecret*: Key32
    permPublic*: Key32
    serverPub*: Key32
    ephSecret*: Key32
    ephPublic*: Key32
    peerEphPublic*: Key32
    peerPermPublic*: Key32
    masterKey*: array[64, uint8]
    sendKey*: Key32
    recvKey*: Key32
    sendCtr*: uint64
    recvCtr*: uint64

proc generateCurveKeypair*(): tuple[secret: Key32, public: Key32] =
  let kp = x25519KeyPair()
  result.secret = kp[0]
  result.public = kp[1]

# ── Key derivation ──────────────────────────────────────────────────────────

proc deriveKeys(cs: CurveState; ecdhEE, ecdhCE, ecdhSE: Key32) =
  var h = initBlake2b(64)
  h.update(ecdhEE)
  h.update(ecdhCE)
  h.update(ecdhSE)
  let mk = h.finish()
  copyMem(addr cs.masterKey[0], unsafeAddr mk[0], 64)
  # client→server key: first 32 bytes of master
  copyMem(addr cs.sendKey[0], addr cs.masterKey[0], 32)
  # server→client key: second 32 bytes of master
  copyMem(addr cs.recvKey[0], addr cs.masterKey[32], 32)

# ── Encryption primitives ───────────────────────────────────────────────────

proc toBytesBE(v: uint64): array[8, uint8] =
  for i in 0..7:
    result[i] = byte((v shr ((7 - i) * 8)) and 0xFF)

proc makeNonce(counter: uint64): array[24, uint8] =
  let be = toBytesBE(counter)
  copyMem(addr result[0], addr be[0], 8)

proc curveEncrypt(key: Key32; ctr: var uint64; data: var seq[byte]): bool =
  let nonce = makeNonce(ctr)
  let (cipher, mac) = encrypt(data, key, nonce)
  data = concat(cipher, @(mac))
  inc ctr
  true

proc curveDecrypt(key: Key32; ctr: var uint64; data: var seq[byte]): bool =
  if data.len < 16: return false
  let nonce = makeNonce(ctr)
  let origLen = data.len - 16
  var mac: Mac16
  copyMem(addr mac[0], addr data[origLen], 16)
  try:
    data = decrypt(data[0 ..< origLen], mac, key, nonce)
    inc ctr
    true
  except ValueError:
    false

# ── Property helpers ────────────────────────────────────────────────────────

proc parseCmdName(body: openArray[byte]): tuple[name: string, dataOff: int] =
  if body.len == 0: return ("", 0)
  var i = 0
  while i < body.len and body[i] != 0: inc i
  result.name = newString(i)
  if i > 0: copyMem(addr result.name[0], unsafeAddr body[0], i)
  result.dataOff = if i < body.len: i + 1 else: body.len

proc parseProps(body: openArray[byte]): (string, string) =
  if body.len == 0: return ("", "")
  var i = 0
  let d = cast[ptr UncheckedArray[byte]](unsafeAddr body[0])
  var sType = ""
  var ident = ""
  while i < body.len:
    let keyLen = d[i].int; inc i
    if i + keyLen > body.len: break
    let key = cast[string](newString(keyLen))
    copyMem(addr key[0], addr d[i], keyLen); i += keyLen
    if i + 4 > body.len: break
    let valLen = (d[i].int shl 24) or (d[i+1].int shl 16) or
                 (d[i+2].int shl 8) or d[i+3].int; i += 4
    if i + valLen > body.len: break
    let val = cast[string](newString(valLen))
    copyMem(addr val[0], addr d[i], valLen); i += valLen
    if key == "Socket-Type": sType = val
    elif key == "Identity": ident = val
  (sType, ident)

proc buildProps(socketType: string): seq[byte] =
  result = newSeq[byte]()
  let stProp = "Socket-Type"
  result.add(byte(stProp.len))
  for c in stProp: result.add(byte(c))
  result.add(byte((socketType.len shr 24) and 0xFF))
  result.add(byte((socketType.len shr 16) and 0xFF))
  result.add(byte((socketType.len shr 8) and 0xFF))
  result.add(byte(socketType.len and 0xFF))
  for c in socketType: result.add(byte(c))

# ── Crypto hook installers ──────────────────────────────────────────────────

proc installCryptoHooks(zc: ZmtpConnection; cs: CurveState) =
  if cs.isServer:
    zc.mechEncrypt = proc(zc: ZmtpConnection; data: var seq[byte]): bool =
      curveEncrypt(cs.recvKey, cs.recvCtr, data)
    zc.mechDecrypt = proc(zc: ZmtpConnection; data: var seq[byte]): bool =
      curveDecrypt(cs.sendKey, cs.sendCtr, data)
  else:
    zc.mechEncrypt = proc(zc: ZmtpConnection; data: var seq[byte]): bool =
      curveEncrypt(cs.sendKey, cs.sendCtr, data)
    zc.mechDecrypt = proc(zc: ZmtpConnection; data: var seq[byte]): bool =
      curveDecrypt(cs.recvKey, cs.recvCtr, data)
  zc.mechDestroy = proc(zc: ZmtpConnection) =
    cs.sendKey.wipe()
    cs.recvKey.wipe()
    cs.masterKey.wipe()
    cs.permSecret.wipe()
    cs.ephSecret.wipe()

# ── Public API ──────────────────────────────────────────────────────────────

proc setupCurveHandshake*(zc: ZmtpConnection; cs: CurveState) =
  zc.mechData = cs
  zc.mechanism = "CURVE"
  zc.mechHandshake = proc(zc: ZmtpConnection; body: openArray[byte]): bool =
    let cs = CurveState(zc.mechData)
    if cs.isServer:
      case cs.phase
      of cpInit:
        if body.len == 0: return true
        let (cmdName, dataOff) = parseCmdName(body)
        if cmdName != "HELLO": return false
        let dataLen = body.len - dataOff
        if dataLen < 64: return false
        copyMem(addr cs.peerPermPublic[0], unsafeAddr body[dataOff], 32)
        copyMem(addr cs.peerEphPublic[0], unsafeAddr body[dataOff + 32], 32)
        (cs.ephSecret, cs.ephPublic) = x25519KeyPair()
        var ecdhEE, ecdhCE, ecdhSE: Key32
        ecdhEE = sharedSecret(cs.ephSecret, cs.peerEphPublic).data
        ecdhCE = sharedSecret(cs.ephSecret, cs.peerPermPublic).data
        ecdhSE = sharedSecret(cs.permSecret, cs.peerEphPublic).data
        deriveKeys(cs, ecdhEE, ecdhCE, ecdhSE)
        discard zc.sendCommand("WELCOME", @(cs.ephPublic))
        cs.phase = cpWaitInitiate
      of cpWaitInitiate:
        let (cmdName, dataOff) = parseCmdName(body)
        if cmdName != "INITIATE": return false
        var encData = @(body.toOpenArray(dataOff, body.high))
        if not curveDecrypt(cs.sendKey, cs.sendCtr, encData): return false
        let (sType, ident) = parseProps(encData)
        if sType.len > 0: zc.peerSocketType = sType
        if ident.len > 0: zc.identity = ident
        var readyBody = buildProps(zc.socketType)
        if not curveEncrypt(cs.recvKey, cs.recvCtr, readyBody): return false
        discard zc.sendCommand("READY", readyBody)
        installCryptoHooks(zc, cs)
        zc.state = ZmtpEstablished
        if zc.onReady != nil: zc.onReady(zc)
      else: discard
    else:
      case cs.phase
      of cpInit:
        if cs.permPublic == default(array[32, uint8]):
          (cs.permSecret, cs.permPublic) = x25519KeyPair()
        (cs.ephSecret, cs.ephPublic) = x25519KeyPair()
        discard zc.sendCommand("HELLO", concat(@(cs.permPublic), @(cs.ephPublic)))
        cs.phase = cpWaitWelcome
      of cpWaitWelcome:
        if body.len < 9: return false
        let (cmdName, dataOff) = parseCmdName(body)
        if cmdName != "WELCOME": return false
        let dataLen = body.len - dataOff
        if dataLen < 32: return false
        copyMem(addr cs.peerEphPublic[0], unsafeAddr body[dataOff], 32)
        var ecdhEE, ecdhCE, ecdhSE: Key32
        ecdhEE = sharedSecret(cs.ephSecret, cs.peerEphPublic).data
        ecdhCE = sharedSecret(cs.permSecret, cs.peerEphPublic).data
        ecdhSE = sharedSecret(cs.ephSecret, cs.serverPub).data
        deriveKeys(cs, ecdhEE, ecdhCE, ecdhSE)
        var initBody = buildProps(zc.socketType)
        if not curveEncrypt(cs.sendKey, cs.sendCtr, initBody): return false
        discard zc.sendCommand("INITIATE", initBody)
        cs.phase = cpWaitReady
      of cpWaitReady:
        let (cmdName, dataOff) = parseCmdName(body)
        if cmdName != "READY": return false
        var encData = @(body.toOpenArray(dataOff, body.high))
        if not curveDecrypt(cs.recvKey, cs.recvCtr, encData): return false
        let (sType, ident) = parseProps(encData)
        if sType.len > 0: zc.peerSocketType = sType
        if ident.len > 0: zc.identity = ident
        installCryptoHooks(zc, cs)
        zc.state = ZmtpEstablished
        if zc.onReady != nil: zc.onReady(zc)
      else: discard
    true

proc setCurveKeypair*(zc: ZmtpConnection; publicKey, secretKey: Key32) =
  var cs: CurveState
  if zc.mechData != nil and zc.mechData of CurveState:
    cs = CurveState(zc.mechData)
  else:
    cs = CurveState(isServer: zc.asServer, phase: cpInit)
  cs.permPublic = publicKey
  cs.permSecret = secretKey
  setupCurveHandshake(zc, cs)

proc setCurveServerKey*(zc: ZmtpConnection; serverKey: Key32) =
  var cs: CurveState
  if zc.mechData != nil and zc.mechData of CurveState:
    cs = CurveState(zc.mechData)
  else:
    cs = CurveState(isServer: false, phase: cpInit)
  cs.serverPub = serverKey
  setupCurveHandshake(zc, cs)

proc setCurveClient*(zc: ZmtpConnection; publicKey, secretKey, serverKey: Key32) =
  var cs = CurveState(isServer: false, phase: cpInit)
  cs.permPublic = publicKey
  cs.permSecret = secretKey
  cs.serverPub = serverKey
  setupCurveHandshake(zc, cs)
