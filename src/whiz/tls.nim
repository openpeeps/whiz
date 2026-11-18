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

import powpow/[loop, types, net/tcp]
import powpow/net/tls as pwtls
import powpow/net/common

export loop, types, tcp, common
from powpow/net/tls import SslContext, SslError
export SslContext, SslError

type
  TlsConfig* = ref object
    serverCtx*: pwtls.SslContext
    clientCtx*: pwtls.SslContext
    serverName*: string

proc enableTlsServer*(cfg: var TlsConfig; certFile, keyFile: string) =
  ## Builds a server TLS context from PEM certificate and key files.
  ## Raises SslError when the cert and key do not match. POSIX only.
  if cfg == nil: cfg = TlsConfig()
  cfg.serverCtx = pwtls.newServerTlsContext(certFile, keyFile)

proc enableTlsClient*(cfg: var TlsConfig; verifyPeer = true; serverName = "") =
  ## Builds a client TLS context. Pass verifyPeer=false for self-signed
  ## test certificates. serverName enables SNI and hostname verification.
  if cfg == nil: cfg = TlsConfig()
  cfg.clientCtx = pwtls.newClientTlsContext(verifyPeer)
  cfg.serverName = serverName

proc setTlsAlpn*(cfg: TlsConfig; protos: openArray[string]) =
  ## Advertises ALPN protocols on both server and client contexts
  ## (e.g. `["zmtp/3.0"]`). No-op for nil contexts. POSIX only.
  if cfg == nil: return
  if cfg.serverCtx != nil:
    cfg.serverCtx.setAlpnProtocols(protos)
  if cfg.clientCtx != nil:
    cfg.clientCtx.setAlpnProtocols(protos)

proc configureTlsKtls*(cfg: TlsConfig; mode: KtlsMode;
                       zerocopySendfile = false) =
  ## Selects kernel-TLS offload policy for subsequently wrapped
  ## connections. `KtlsAuto` tries offload with userspace fallback,
  ## `KtlsRequired` fails closed, `KtlsOff` disables. Only meaningful
  ## on Linux with OpenSSL `enable-ktls`; safe no-op elsewhere.
  if cfg == nil: return
  if cfg.serverCtx != nil:
    cfg.serverCtx.configureKtls(mode, zerocopySendfile)
  if cfg.clientCtx != nil:
    cfg.clientCtx.configureKtls(mode, zerocopySendfile)

proc enableTlsKtls*(cfg: TlsConfig; zerocopySendfile = false) =
  ## Best-effort kTLS (`KtlsAuto`). Shorthand for
  ## `configureTlsKtls(cfg, KtlsAuto, zerocopySendfile)`.
  cfg.configureTlsKtls(KtlsAuto, zerocopySendfile)

proc disableTlsKtls*(cfg: TlsConfig) =
  ## Stops requesting kTLS offload for subsequently wrapped connections.
  cfg.configureTlsKtls(KtlsOff)

proc ktlsSupported*(): bool =
  ## True when this kernel can attach the TLS ULP (Linux with `tls`
  ## module). Always false off Linux.
  pwtls.ktlsSupported()

proc isTlsActive*(conn: Connection): bool =
  ## True once the OpenSSL handshake completed on this connection.
  pwtls.isTlsActive(conn)

proc ktlsActive*(conn: Connection): bool =
  ## True when either TX or RX direction is currently offloaded to kTLS.
  pwtls.ktlsActive(conn)

proc ktlsTxActive*(conn: Connection): bool =
  pwtls.ktlsTxActive(conn)

proc ktlsRxActive*(conn: Connection): bool =
  pwtls.ktlsRxActive(conn)

proc tlsAlpnSelected*(conn: Connection): string =
  ## Negotiated ALPN protocol, or "" when none was negotiated.
  pwtls.alpnSelected(conn)

proc wrapServerConn*(conn: Connection; cfg: TlsConfig): bool =
  ## Upgrades an accepted connection to TLS. Returns false when setup
  ## failed, in which case the connection is already closed.
  if cfg == nil or cfg.serverCtx == nil: return true
  try:
    conn.wrapTls(cfg.serverCtx)
    true
  except pwtls.SslError:
    conn.close()
    false

proc wrapClientConn*(conn: Connection; cfg: TlsConfig): bool =
  ## Upgrades an outbound connection to TLS. Returns false when setup
  ## failed, in which case the connection is already closed.
  if cfg == nil or cfg.clientCtx == nil: return true
  try:
    conn.wrapTls(cfg.clientCtx, cfg.serverName)
    true
  except pwtls.SslError:
    conn.close()
    false
