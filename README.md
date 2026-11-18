<p align="center">
  WhizMQ - A message queue library implementing ZMTP 3.0 in Nim.<br>
  Built on top of <a href="https://github.com/openpeeps/powpow">PowPow event library</a>
</p>

<p align="center">
  <code>nimble install whiz</code>
</p>

<p align="center">
  <a href="https://openpeeps.github.io/whiz">API reference</a><br>
  <img src="https://github.com/openpeeps/whiz/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/openpeeps/whiz/workflows/docs/badge.svg" alt="Github Actions">
</p>

## 😍 Key Features
- **ZMTP 3.0** wire protocol, full greeting, framing, and command handling
- **Socket patterns**: PAIR, PUB/SUB, REQ/REP, PUSH/PULL, CLIENT/SERVER, File Transfer
- **Security mechanisms**:
  - NULL: no authentication (plaintext)
  - PLAIN: username/password authentication with ZAP callback
  - CURVE: X25519 + XChaCha20-Poly1305 AEAD encryption and mutual authentication
- **Transport** TCP (with optional TLS on POSIX), IPC (Unix domain sockets), and WebSocket (RFC 6455)
- **Built on [PowPow](https://github.com/openpeeps/powpow)** event notification library in Nim

> [!NOTE]
> CURVE is powered by [nimcypher](https://github.com/openpeeps/nimcypher), a 100% pure-Nim port of Monocypher 4.0.3 — no C compiler flags, no system libraries to install.

## 🗺 Roadmap

- [x] ZMTP 3.0 wire protocol (greeting, framing, commands)
- [x] PAIR, PUB/SUB, REQ/REP, PUSH/PULL socket patterns
- [x] File transfer over ZMTP
- [x] PLAIN security mechanism + ZAP auth callbacks
- [x] CURVE security mechanism (X25519 + AEAD)
- [ ] RADIO/DISH (dgram) socket pattern
- [x] CLIENT/SERVER (stream) socket pattern
- [x] CURVE vouch (Ed25519 signature in HELLO for key continuity)
- [x] TLS transport (TCP, POSIX) + kTLS/ALPN options
- [ ] DTLS transport
- [ ] GSSAPI / Kerberos mechanism
- [x] WebSocket transport (RFC 6455 + permessage-deflate/ping options)
- [ ] Formal ZMTP conformance tests
- [x] Performance benchmarks vs ØMQ (see below)


## Examples

Runnable programs live in [`examples/`](examples/) (each file runs standalone).
Build with clue (`clue build examples/pair.nim -o:/tmp/ex-pair`) or plain Nim
(`nim c -r examples/pair.nim`):

| File | What it shows |
|---|---|
| `examples/pair.nim` | PAIR 1-to-1 bidirectional messaging |
| `examples/pubsub.nim` | PUB/SUB with topic filtering |
| `examples/reqrep.nim` | REQ/REP strict request-reply |
| `examples/pushpull.nim` | PUSH/PULL pipeline |
| `examples/clientserver.nim` | CLIENT/SERVER with routing handles |
| `examples/curve.nim` | CURVE encrypted PAIR (X25519 + AEAD) |
| `examples/curve_vouch.nim` | CURVE + Ed25519 vouch (key continuity) |
| `examples/tls.nim` | PAIR over TLS + ALPN/kTLS options (POSIX, needs `openssl` CLI) |
| `examples/ws_pair.nim` | PAIR over WebSocket + deflate/ping options |
| `examples/filetx.nim` | File transfer over a PAIR connection |

PAIR:

```nim
import whiz/pair
let loop = newLoop()
let srv = newPairSocket(loop)
srv.`bind`("127.0.0.1", 5550)
let cli = newPairSocket(loop)
srv.onMessage = proc(data: openArray[byte]) =
  discard srv.send("hello from server")
cli.onMessage = proc(data: openArray[byte]) =
  loop.stop()
discard loop.addTimer(50) do (id: int):
  cli.connect("127.0.0.1", 5550)
discard loop.addTimer(250) do (id: int):
  discard cli.send("hello from client")
loop.run()
```

REQ/REP:

```nim
import whiz/reqrep
let loop = newLoop()
let rep = newRepSocket(loop)
rep.`bind`("127.0.0.1", 5552)
let req = newReqSocket(loop)
rep.onRequest = proc(data: openArray[byte]) =
  discard rep.send("reply")
req.onReply = proc(data: openArray[byte]) =
  loop.stop()
discard loop.addTimer(50) do (id: int):
  req.connect("127.0.0.1", 5552)
discard loop.addTimer(250) do (id: int):
  discard req.send("hello")
loop.run()
```

CURVE + vouch:

```nim
import whiz/pair, whiz/curve
let (srvSec, srvPub) = generateCurveKeypair()
let (cliSec, cliPub) = generateCurveKeypair()
let (vouchSec, vouchPub) = generateVouchKeypair()
let loop = newLoop()
let srv = newPairSocket(loop)
srv.setCurveKeypair(srvPub, srvSec)
srv.setCurveExpectedVouch(vouchPub) # reject HELLOs without this vouch
srv.`bind`("127.0.0.1", 5556)
let cli = newPairSocket(loop)
cli.setCurveClient(cliPub, cliSec, srvPub)
cli.setCurveVouch(vouchPub, vouchSec)
```

### Benchmarks
> [!NOTE]
> Benchmark results are not consistent and may show different results across runs and environments.

### vs ØMQ (C libzmq 5.2.5 via pkg/zmq)

Same scenarios, same payloads, same machine, back-to-back (`tests/bench_vs_zmq.nim`,
`DYLD_LIBRARY_PATH=/opt/local/lib nim c -d:release -r tests/bench_vs_zmq.nim`).
whiz peers share one event loop; ØMQ uses its blocking API with a helper
thread per bench. Loopback TCP.

| impl | benchmark | n | size | total(μs) | throughput/s | min(μs) | p50(μs) | p75(μs) | p90(μs) | p99(μs) | avg(μs) | σ(μs) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| whiz | pair_latency | 5000 | 1 | 144720 | 34549 | 23.9 | 27.3 | 28.9 | 33.1 | 44.1 | 28.8 | 276.0 |
| whiz | pair_throughput | 10000 | 512 | 11563 | 864755 | 0.0 | 0.0 | 0.1 | 1.3 | 1.9 | 0.3 | 374.0 |
| whiz | reqrep | 5000 | 128 | 147627 | 33869 | 26.0 | 27.7 | 29.7 | 34.2 | 45.4 | 29.4 | 290.7 |
| whiz | pushpull_1worker | 10000 | 128 | 2187 | 4570567 | 0.0 | 0.0 | 0.0 | 0.0 | 1.6 | 0.1 | 202.3 |
| zmq | pair_latency | 5000 | 1 | 510731 | 9789 | 89.7 | 97.4 | 107.0 | 119.0 | 147.7 | 102.1 | 1593.6 |
| zmq | pair_throughput | 10000 | 512 | 6344 | 1576206 | 0.2 | 0.2 | 0.2 | 0.3 | 17.8 | 0.6 | 336.4 |
| zmq | reqrep | 5000 | 128 | 551512 | 9065 | 92.5 | 104.0 | 114.3 | 128.9 | 204.0 | 110.2 | 2069.8 |
| zmq | pushpull_1worker | 10000 | 128 | 4296 | 2327689 | 0.2 | 0.2 | 0.2 | 0.3 | 9.8 | 0.4 | 179.9 |

**Takeaways:**
- **Round-trips favor whiz ~3.5×** (pair_latency 35k vs 9.8k/s; reqrep 34k vs 9.1k/s) — single event loop, no context-switch or helper-thread hops.
- **Floods are now split**: 128B PUSH/PULL favors whiz ~2× (4.57M vs 2.33M/s); 512B pair floods still favor ØMQ ~1.8× (1.58M vs 865k/s, down from 2.6× before batching + zero-copy receive).
- Different APIs (event-loop vs blocking+threads), so treat this as a directional comparison, not a certified benchmark.

### Performance notes

Three profile-driven optimizations (xctrace + `sample` on loopback floods),
all verified with `nimble benchVsZmq` and `nimble benchMem`:

- **Write batching** (`setBatching` on any socket, off by default). One
  `writev` per message dominated profiles (~60% of active time), so batched
  sends coalesce into the write buffer and drain in bulk on the next loop
  pass. Enable for floods; leave off for lockstep latency paths, which
  already hit the wire immediately.
  - pair_throughput 512B: 467k → **865k/s** (+85%)
  - pushpull_1worker 128B: 1.74M → **4.57M/s** (+163%)
  - pair_latency: unchanged behavior by default (batching off)
- **Zero-copy receive** (`feed` fast path). Fully-buffered frames used to
  be copied into `recvBuf` before parsing; they are now parsed straight
  from the read chunk, staging only a trailing partial frame. Part of the
  gains above; covered by `tests/test_feed.nim` (split/long/multi-frame).
- **Single-send tiny frames.** Payloads ≤ 64B go out as one contiguous
  stack buffer + `send` instead of a 2-part `writev` (skips iovec setup).
  - pair_latency avg: 27.9 → **26.9μs** (~+4% round-trip rate)

Memory stays flat by construction: per-connection pools grow once to the
largest frame then stay flat, and a repeated 20k-message flood retains
~0 B/message on round two (`tests/bench_mem.nim`).

**Column legend:**
- `benchmark` — test name: `{pattern}_{scenario}_{transport}` (`_tcp` or `_unix`)
- `subscriber` — specific subscriber/worker for multi-endpoint tests; `total` is the aggregate row, `-` for single-endpoint
- `n` — number of messages sent (throughput) or round-trips (latency)
- `size` — payload size in bytes
- `total(μs)` — wall-clock duration of the test in microseconds
- `throughput/s` — messages per second = `n / total(μs) * 1_000_000`
- `min(μs)` — fastest observed per-message time (minimum)
- `p50(μs)` — median per-message time (50th percentile)
- `p75(μs)` — 75th percentile per-message time
- `p90(μs)` — 90th percentile per-message time
- `p99(μs)` — 99th percentile per-message time
- `avg(μs)` — arithmetic mean per-message time
- `σ(μs)` — population standard deviation of per-message times

**Per-message time collection:**
- **Latency benchmarks** (`pair_latency`, `reqrep`): each value is one request–reply round-trip measured from the client
- **Throughput benchmarks** (all others): inter-arrival deltas between consecutive receives — approximates per-message service time distribution
- **`bench_sizes_pair`** uses ACK-windowed flow control: the PAIR server echoes a 1-byte ack every 250 messages and the client keeps at most 250 unacked messages in flight, so bulk transfers measure *delivered* throughput instead of buffer absorption
- Every benchmark arms a watchdog timer: if the run stalls, results are discarded with a diagnostic instead of hanging the suite

| benchmark | subscriber | n | size | total(μs) | throughput/s | min(μs) | p50(μs) | p75(μs) | p90(μs) | p99(μs) | avg(μs) | σ(μs) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| bench_pair_latency_tcp | - | 5000 | 1 | 137204 | 36441 | 24.1 | 26.9 | 27.2 | 27.5 | 52.7 | 27.4 | 4.8 |
| bench_pair_latency_unix | - | 5000 | 1 | 31419 | 159136 | 5.8 | 6.1 | 6.2 | 6.2 | 8.6 | 6.2 | 1.0 |
| bench_pair_throughput_tcp | - | 10000 | 512 | 16079 | 621913 | 0.0 | 0.0 | 0.1 | 1.3 | 3.1 | 0.3 | 4.2 |
| bench_pair_throughput_unix | - | 10000 | 512 | 10471 | 954947 | 0.0 | 0.0 | 0.1 | 1.4 | 7.0 | 0.6 | 1.7 |
| bench_pubsub_1sub_tcp | - | 10000 | 512 | 12556 | 796406 | 0.0 | 0.0 | 0.1 | 1.4 | 3.4 | 0.4 | 3.4 |
| bench_pubsub_1sub_unix | - | 10000 | 512 | 10044 | 995561 | 0.0 | 0.0 | 0.1 | 1.3 | 6.5 | 0.5 | 1.6 |
| bench_pubsub_2sub_tcp | sub0 | 5000 | 256 | 20016 | 249790 | 0.0 | 0.0 | 0.0 | 0.1 | 3.8 | 0.4 | 11.7 |
| bench_pubsub_2sub_tcp | sub1 | 5000 | 256 | 20016 | 249790 | 0.0 | 0.0 | 0.0 | 0.1 | 3.7 | 0.3 | 9.9 |
| bench_pubsub_2sub_tcp | total | 10000 | 256 | 20016 | 499581 | 0.0 | 0.0 | 0.0 | 0.1 | 3.8 | 0.3 | 10.8 |
| bench_pubsub_2sub_unix | sub0 | 5000 | 256 | 4391 | 1138471 | 0.0 | 0.0 | 0.1 | 0.1 | 14.2 | 0.5 | 2.5 |
| bench_pubsub_2sub_unix | sub1 | 5000 | 256 | 4391 | 1138471 | 0.0 | 0.0 | 0.0 | 0.1 | 14.3 | 0.5 | 2.5 |
| bench_pubsub_2sub_unix | total | 10000 | 256 | 4391 | 2276942 | 0.0 | 0.0 | 0.1 | 0.1 | 14.2 | 0.5 | 2.5 |
| bench_reqrep_tcp | - | 5000 | 128 | 146617 | 34102 | 26.2 | 27.9 | 28.9 | 30.9 | 47.8 | 29.2 | 4.1 |
| bench_reqrep_unix | - | 5000 | 128 | 33862 | 147656 | 6.3 | 6.5 | 6.6 | 6.7 | 9.3 | 6.7 | 1.3 |
| bench_sizes_pair_tcp | - | 2000 | 64 | 9345 | 214016 | 0.0 | 0.0 | 0.0 | 0.0 | 3.5 | 4.7 | 72.2 |
| bench_sizes_pair_tcp | - | 2000 | 1024 | 11770 | 169915 | 0.0 | 0.1 | 1.7 | 1.9 | 3.2 | 5.8 | 84.7 |
| bench_sizes_pair_tcp | - | 2000 | 65536 | 114554 | 17458 | 20.2 | 21.9 | 24.0 | 27.6 | 497.9 | 57.3 | 564.0 |
| bench_sizes_pair_unix | - | 2000 | 64 | 1673 | 1194801 | 0.0 | 0.1 | 0.1 | 0.1 | 7.0 | 0.8 | 11.2 |
| bench_sizes_pair_unix | - | 2000 | 1024 | 3158 | 633231 | 0.0 | 0.1 | 1.3 | 6.6 | 9.8 | 1.6 | 7.7 |
| bench_sizes_pair_unix | - | 2000 | 65536 | 186151 | 10743 | 61.0 | 63.3 | 64.2 | 71.5 | 140.2 | 93.1 | 520.0 |
| bench_pushpull_throughput_tcp | - | 10000 | 512 | 10939 | 914088 | 0.0 | 0.0 | 0.1 | 1.4 | 3.4 | 0.3 | 3.4 |
| bench_pushpull_throughput_unix | - | 10000 | 512 | 8595 | 1163403 | 0.0 | 0.0 | 0.1 | 1.4 | 6.7 | 0.6 | 1.7 |
| bench_pushpull_1worker_tcp | - | 10000 | 128 | 4930 | 2028354 | 0.0 | 0.0 | 0.0 | 0.0 | 1.6 | 0.1 | 2.3 |
| bench_pushpull_1worker_unix | - | 10000 | 128 | 3071 | 3255314 | 0.0 | 0.0 | 0.0 | 0.0 | 0.1 | 0.2 | 0.8 |
| bench_pushpull_3workers_tcp | worker0 | 1667 | 128 | 8667 | 192326 | 0.0 | 0.0 | 0.0 | 0.0 | 4.2 | 0.3 | 8.1 |
| bench_pushpull_3workers_tcp | worker1 | 1667 | 128 | 8667 | 192326 | 0.0 | 0.0 | 0.0 | 0.0 | 3.9 | 0.3 | 7.8 |
| bench_pushpull_3workers_tcp | worker2 | 1666 | 128 | 8667 | 192211 | 0.0 | 0.0 | 0.0 | 0.0 | 4.2 | 0.3 | 7.4 |
| bench_pushpull_3workers_tcp | total | 5000 | 128 | 8667 | 576865 | 0.0 | 0.0 | 0.0 | 0.0 | 3.9 | 0.3 | 7.8 |
| bench_pushpull_3workers_unix | worker0 | 1667 | 128 | 1711 | 973920 | 0.0 | 0.0 | 0.0 | 0.1 | 23.1 | 0.5 | 3.1 |
| bench_pushpull_3workers_unix | worker1 | 1667 | 128 | 1711 | 973920 | 0.0 | 0.0 | 0.0 | 0.1 | 23.1 | 0.4 | 3.1 |
| bench_pushpull_3workers_unix | worker2 | 1666 | 128 | 1711 | 973336 | 0.0 | 0.0 | 0.0 | 0.0 | 23.0 | 0.4 | 3.0 |
| bench_pushpull_3workers_unix | total | 5000 | 128 | 1711 | 2921176 | 0.0 | 0.0 | 0.0 | 0.1 | 23.1 | 0.4 | 3.1 |

**Key observations:**
- **Unix domain sockets win big on latency** — round-trip benchmarks are ~4.4× faster on Unix (pair_latency avg 6.2μs vs 27.4μs; reqrep 6.7μs vs 29.2μs) with tighter jitter.
- **Bulk 64 KiB transfers favor TCP under backpressure but the gap closed** — with ACK-windowed flow control, 65536B pair delivery reaches ~17.5k msgs/s over TCP vs ~10.7k/s over Unix (1.6×; avg inter-arrival gap 57μs vs 93μs).
- **Small-message floods saturate around 600k–1.2M msgs/s** per connection for 512B payloads; PUSH/PULL with 128B frames to a single worker exceeds 2M msgs/s on TCP and 3.2M msgs/s on Unix.
- **Multi-worker PUSH/PULL divides fairly but adds per-worker overhead** — 3 workers aggregate ~577k msgs/s on TCP vs 2.9M msgs/s on Unix.
- **Pub/sub fan-out is balanced on both transports** — `pubsub_2sub` delivers 5000/5000 per subscriber on TCP and Unix alike.
- **Backpressure-safe by construction** — flood volumes beyond powpow's `maxWriteBufferSize` cap (default 32MB, tunable via `-d:maxWriteBufferSize=N` in MB) are handled by ACK windows + watchdog timers instead of silent connection death.

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/whiz/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/whiz/fork)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
