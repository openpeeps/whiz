# Whiz Message Queue — A message queue library implementing ZMTP 3.0 in Nim.
#
# (c) 2025 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/whiz

## Implement specific socket patterns from the ZeroMQ ecosystem on top
## of ZMTP 3.0 wire protocol. Built on top of PowPow event library.
##
## Import the specific socket you need:
##   - `import whiz/pair`       — Exclusive PAIR (1-to-1 bidirectional)
##   - `import whiz/pubsub`     — PUB/SUB (publish-subscribe)
##   - `import whiz/reqrep`     — REQ/REP (request-reply)
##   - `import whiz/pushpull`   — PUSH/PULL (pipeline)
##   - `import whiz/clientserver` — CLIENT/SERVER (stream)
##   - `import whiz/zmtp`       — ZMTP 3.0 wire protocol primitives
##   - `import whiz/filetx`     — File transfer over ZMTP
##   - `import whiz/ws`         — WebSocket transport (all patterns over WS)
##   - `import whiz/tls`        — TLS transport config (TCP, POSIX only)
##
## Or import everything at once with:
##   `import whiz/[zmtp, pair, pubsub, reqrep, pushpull, clientserver, filetx, ws]`

when isMainModule:
  echo "WhizMQ — ZMTP 3.0 message queue library"
else:
  error("Cannot import `whiz` directly. Import a specific sub-module instead:\n" &
          "  import whiz/pair      — Exclusive PAIR (1-to-1 bidirectional)\n" &
          "  import whiz/pubsub    — PUB/SUB (publish-subscribe)\n" &
          "  import whiz/reqrep    — REQ/REP (request-reply)\n" &
           "  import whiz/pushpull  — PUSH/PULL (pipeline)\n" &
           "  import whiz/clientserver — CLIENT/SERVER (stream)\n" &
          "  import whiz/zmtp      — ZMTP 3.0 wire protocol primitives\n" &
          "  import whiz/filetx    — File transfer over ZMTP\n" &
          "  import whiz/ws        — WebSocket transport\n")
