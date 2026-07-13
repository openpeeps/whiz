# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## PLAIN security mechanism and ZAP authentication framework.
##
## Implements the ZMTP PLAIN security mechanism (RFC 27) using
## username/password credentials, and provides ZAP callback hooks.

import ./zmtp

export zmtp

type
  ZapCredentials* = object
    username*: string
    password*: string

  ZapHandler* = proc(credentials: ZapCredentials): bool {.closure.}

  ZapState* = ref object of RootObj
    username: string
    password: string
    zapHandler: ZapHandler
    isServer: bool
    readySent: bool

  SocketAuth* = ref object
    mechanism*: string
    plainUser*, plainPass*: string
    pubKey*, secKey*, srvKey*: array[32, uint8]
    zapHandler*: ZapHandler

proc parseCmdName(body: openArray[byte]; pos: int = 0): tuple[name: string, dataOff: int] =
  if body.len <= pos: return ("", pos)
  var i = pos
  while i < body.len and body[i] != 0: inc i
  result.name = newString(i - pos)
  if result.name.len > 0:
    copyMem(addr result.name[0], unsafeAddr body[pos], i - pos)
  result.dataOff = if i < body.len: i + 1 else: body.len

proc findProp(body: openArray[byte]; pos: int; name: string): (string, int) =
  var i = pos
  let d = cast[ptr UncheckedArray[byte]](unsafeAddr body[0])
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
    copyMem(addr val[0], addr d[i], valLen)
    if key == name:
      return (val, i)
    i += valLen
  ("", i)

proc setPlainAuth*(zc: ZmtpConnection; username, password: string) =
  var zs = ZapState(isServer: zc.asServer, username: username, password: password)
  zc.mechData = zs

  zc.mechHandshake = proc(zc: ZmtpConnection; body: openArray[byte]): bool =
    let zs = ZapState(zc.mechData)
    if zs.isServer:
      if body.len == 0: return true
      let (cmdName, dataOff) = parseCmdName(body, 0)
      if cmdName != "READY": return false
      if dataOff >= body.len: return false
      let (uname, _) = findProp(body, dataOff, "Username")
      zs.username = uname
      let (pass, _) = findProp(body, dataOff, "Password")
      zs.password = pass
      if zs.zapHandler != nil:
        let creds = ZapCredentials(username: zs.username, password: zs.password)
        if not zs.zapHandler(creds):
          discard zc.sendCommand("ERROR", "Invalid credentials")
          zc.close()
          return true
      let (sType, _) = findProp(body, dataOff, "Socket-Type")
      if sType.len > 0: zc.peerSocketType = sType
      let (identity, _) = findProp(body, dataOff, "Identity")
      if identity.len > 0: zc.identity = identity
      zc.sendReady(zc.socketType)
      zc.state = ZmtpEstablished
      if zc.onReady != nil: zc.onReady(zc)
    else:
      if body.len == 0 and not zs.readySent:
        zs.readySent = true
        var bodySeq = newSeq[byte]()
        let userProp = "Username"
        bodySeq.add(byte(userProp.len))
        for c in userProp: bodySeq.add(byte(c))
        bodySeq.add(byte((zs.username.len shr 24) and 0xFF))
        bodySeq.add(byte((zs.username.len shr 16) and 0xFF))
        bodySeq.add(byte((zs.username.len shr 8) and 0xFF))
        bodySeq.add(byte(zs.username.len and 0xFF))
        for c in zs.username: bodySeq.add(byte(c))
        let passProp = "Password"
        bodySeq.add(byte(passProp.len))
        for c in passProp: bodySeq.add(byte(c))
        bodySeq.add(byte((zs.password.len shr 24) and 0xFF))
        bodySeq.add(byte((zs.password.len shr 16) and 0xFF))
        bodySeq.add(byte((zs.password.len shr 8) and 0xFF))
        bodySeq.add(byte(zs.password.len and 0xFF))
        for c in zs.password: bodySeq.add(byte(c))
        let stProp = "Socket-Type"
        bodySeq.add(byte(stProp.len))
        for c in stProp: bodySeq.add(byte(c))
        bodySeq.add(byte((zc.socketType.len shr 24) and 0xFF))
        bodySeq.add(byte((zc.socketType.len shr 16) and 0xFF))
        bodySeq.add(byte((zc.socketType.len shr 8) and 0xFF))
        bodySeq.add(byte(zc.socketType.len and 0xFF))
        for c in zc.socketType: bodySeq.add(byte(c))
        discard zc.sendCommand("READY", bodySeq)
      else:
        if body.len == 0: return true
        let (cmdName, dataOff) = parseCmdName(body, 0)
        if cmdName == "READY":
          if dataOff < body.len:
            let (sType, _) = findProp(body, dataOff, "Socket-Type")
            if sType.len > 0: zc.peerSocketType = sType
            let (identity, _) = findProp(body, dataOff, "Identity")
            if identity.len > 0: zc.identity = identity
          zc.state = ZmtpEstablished
          if zc.onReady != nil: zc.onReady(zc)
    true

proc onAuthenticate*(zc: ZmtpConnection; handler: ZapHandler) =
  if zc.mechData != nil and zc.mechData of ZapState:
    ZapState(zc.mechData).zapHandler = handler

proc applyAuth*(zc: ZmtpConnection; sa: SocketAuth) =
  if sa == nil or sa.mechanism == "" or sa.mechanism == "NULL": return
  if sa.mechanism == "PLAIN":
    setPlainAuth(zc, sa.plainUser, sa.plainPass)
    if sa.zapHandler != nil:
      onAuthenticate(zc, sa.zapHandler)
