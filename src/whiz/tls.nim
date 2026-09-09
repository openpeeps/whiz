# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## TLS transport support (TCP only, POSIX only).
##
## Powered by powpow's OpenSSL binding. Attach a config to any socket with
## setTlsServer / setTlsClient before bind / connect; the ZMTP greeting
## then flows inside the TLS session with no protocol changes. TLS and
## CURVE compose: CURVE encrypts frame bodies inside the TLS stream.

import powpow/[loop, types, net/tcp, net/tls]

export loop, types, tcp, tls

type
  TlsConfig* = ref object
    serverCtx*: SslContext
    clientCtx*: SslContext
    serverName*: string

proc enableTlsServer*(cfg: var TlsConfig; certFile, keyFile: string) =
  ## Builds a server TLS context from PEM certificate and key files.
  ## Raises SslError when the cert and key do not match. POSIX only.
  if cfg == nil: cfg = TlsConfig()
  cfg.serverCtx = newServerTlsContext(certFile, keyFile)

proc enableTlsClient*(cfg: var TlsConfig; verifyPeer = true; serverName = "") =
  ## Builds a client TLS context. Pass verifyPeer=false for self-signed
  ## test certificates. serverName enables SNI and hostname verification.
  if cfg == nil: cfg = TlsConfig()
  cfg.clientCtx = newClientTlsContext(verifyPeer)
  cfg.serverName = serverName

proc wrapServerConn*(conn: Connection; cfg: TlsConfig): bool =
  ## Upgrades an accepted connection to TLS. Returns false when setup
  ## failed, in which case the connection is already closed.
  if cfg == nil or cfg.serverCtx == nil: return true
  try:
    conn.wrapTls(cfg.serverCtx)
    true
  except SslError:
    conn.close()
    false

proc wrapClientConn*(conn: Connection; cfg: TlsConfig): bool =
  ## Upgrades an outbound connection to TLS. Returns false when setup
  ## failed, in which case the connection is already closed.
  if cfg == nil or cfg.clientCtx == nil: return true
  try:
    conn.wrapTls(cfg.clientCtx, cfg.serverName)
    true
  except SslError:
    conn.close()
    false
