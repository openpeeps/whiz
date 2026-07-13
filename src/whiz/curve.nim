# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## CURVE security mechanism for ZMTP.
##
## Implements the ZMTP CURVE security mechanism using Monocypher's
## X25519 key exchange and XChaCha20-Poly1305 AEAD encryption.

import std/[sequtils]
import e2ee/private/[monocypher, utils]
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
  result = x25519KeyPair()

# ── Key derivation ──────────────────────────────────────────────────────────

proc deriveKeys(cs: CurveState; ecdhEE, ecdhCE, ecdhSE: Key32) =
  var ctx: crypto_blake2b_ctx
  crypto_blake2b_init(addr ctx, 64)
  crypto_blake2b_update(addr ctx, addr ecdhEE[0], 32)
  crypto_blake2b_update(addr ctx, addr ecdhCE[0], 32)
  crypto_blake2b_update(addr ctx, addr ecdhSE[0], 32)
  crypto_blake2b_final(addr ctx, addr cs.masterKey[0])
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
  var mac: Mac16
  var cipher = newSeq[byte](data.len)
  if data.len > 0:
    crypto_aead_lock(
      addr cipher[0], addr mac[0],
      addr key[0], addr nonce[0],
      nil, 0,
      addr data[0], csize_t(data.len)
    )
  data = concat(cipher, @(mac))
  inc ctr
  true

proc curveDecrypt(key: Key32; ctr: var uint64; data: var seq[byte]): bool =
  let nonce = makeNonce(ctr)
  if data.len < 16: return false
  let origLen = data.len - 16
  var plain = newSeq[byte](origLen)
  let res = crypto_aead_unlock(
    if origLen > 0: addr plain[0] else: nil,
    addr data[origLen],
    addr key[0], addr nonce[0],
    nil, 0,
    if origLen > 0: addr data[0] else: nil,
    csize_t(origLen)
  )
  if res != 0: return false
  data = plain
  inc ctr
  true

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
    crypto_wipe(addr cs.sendKey[0], 32)
    crypto_wipe(addr cs.recvKey[0], 32)
    crypto_wipe(addr cs.masterKey[0], 64)
    crypto_wipe(addr cs.permSecret[0], 32)
    crypto_wipe(addr cs.ephSecret[0], 32)

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
        crypto_x25519(addr ecdhEE[0], addr cs.ephSecret[0], addr cs.peerEphPublic[0])
        crypto_x25519(addr ecdhCE[0], addr cs.ephSecret[0], addr cs.peerPermPublic[0])
        crypto_x25519(addr ecdhSE[0], addr cs.permSecret[0], addr cs.peerEphPublic[0])
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
        crypto_x25519(addr ecdhEE[0], addr cs.ephSecret[0], addr cs.peerEphPublic[0])
        crypto_x25519(addr ecdhCE[0], addr cs.permSecret[0], addr cs.peerEphPublic[0])
        crypto_x25519(addr ecdhSE[0], addr cs.ephSecret[0], addr cs.serverPub[0])
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
