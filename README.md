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
| whiz | pair_latency | 5000 | 1 | 137751 | 36297 | 23.4 | 26.5 | 27.2 | 30.0 | 44.7 | 27.5 | 295.8 |
| whiz | pair_throughput | 10000 | 512 | 10225 | 977915 | 0.0 | 0.0 | 0.1 | 1.2 | 1.8 | 0.3 | 413.7 |
| whiz | reqrep | 5000 | 128 | 144299 | 34650 | 25.2 | 27.5 | 28.1 | 31.6 | 48.7 | 28.8 | 333.8 |
| whiz | pushpull_1worker | 10000 | 128 | 1960 | 5101140 | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.1 | 201.6 |
| zmq | pair_latency | 5000 | 1 | 499648 | 10007 | 90.5 | 95.3 | 106.1 | 112.1 | 134.2 | 99.8 | 1311.6 |
| zmq | pair_throughput | 10000 | 512 | 6732 | 1485397 | 0.2 | 0.2 | 0.2 | 0.3 | 25.2 | 0.6 | 346.3 |
| zmq | reqrep | 5000 | 128 | 521682 | 9584 | 93.4 | 99.2 | 109.4 | 118.3 | 140.9 | 104.2 | 1368.1 |
| zmq | pushpull_1worker | 10000 | 128 | 3985 | 2509207 | 0.2 | 0.2 | 0.2 | 0.3 | 6.8 | 0.4 | 168.9 |

**Takeaways:**
- **Round-trips favor whiz ~3.6×** (pair_latency 36k vs 10k/s; reqrep 35k vs 9.6k/s): single event loop, no context-switch or helper-thread hops.
- **Floods are now split**: 128B PUSH/PULL favors whiz ~2× (5.10M vs 2.51M/s); 512B pair floods still favor ØMQ ~1.5× (1.49M vs 978k/s, down from 2.6× before batching, zero-copy receive, and uninitialized buffer growth).
- Different APIs (event-loop vs blocking+threads), so treat this as a directional comparison, not a certified benchmark.

### Performance notes

Six profile-driven optimizations (xctrace + `sample` on loopback floods),
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
  from the read chunk, staging only a trailing partial frame. A split
  frame is completed by copying only its missing tail (at most one frame),
  never the whole chunk. Covered by `tests/test_feed.nim`
  (split/long/multi-frame, split-then-burst).
- **Read sizing** (`-d:powpowReadBufSize=N`, default 4096). One recv fills
  up to the buffer size per Read event, so 4KB reads cost one syscall per
  ~7 x 512B frames. 64KB reads take ~127 frames per recv with no added
  latency and lift sustained floods ~40% (1.98M to 2.85M/s); the
  single-burst vs-zmq pair flood gains ~15% (831k to 945k/s, 1.12M/s with
  both this and the item below).
- **Uninitialized buffer growth** (powpow write paths). Appending to
  `writeBuf` used `setLen` (zero-fill) followed by `copyMem`, paying the
  payload twice in memory traffic per message. Hot append paths now use
  `setLenUninit` since every new byte is overwritten immediately; sustained
  512B floods rise ~85% (3.2M to 6.0M/s). Retention behavior is unchanged
  (`nimble benchMem` round-2 delta still ~80 bytes).
- **Single-send tiny frames.** Payloads ≤ 64B go out as one contiguous
  stack buffer + `send` instead of a 2-part `writev` (skips iovec setup).
  - pair_latency avg: 27.9 → **26.9μs** (~+4% round-trip rate)
- **Socket buffer hints** (`setSendBufferSize`/`setRecvBufferSize`, opt-in,
  applied to current and future connections). Pre-sizing SO_SNDBUF/SO_RCVBUF
  skips part of the kernel autotune ramp on cold bursts (~+1–6% on a
  single 30k×512B flood; sustained loads autotune on their own).

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
| bench_pair_latency_tcp | - | 5000 | 1 | 137271 | 36424 | 23.7 | 26.0 | 27.2 | 30.5 | 47.8 | 27.4 | 4.5 |
| bench_pair_latency_unix | - | 5000 | 1 | 30816 | 162250 | 5.7 | 6.0 | 6.0 | 6.1 | 8.6 | 6.1 | 0.8 |
| bench_pair_throughput_tcp | - | 10000 | 512 | 15012 | 666090 | 0.0 | 0.0 | 0.0 | 1.2 | 3.3 | 0.3 | 4.3 |
| bench_pair_throughput_unix | - | 10000 | 512 | 8456 | 1182456 | 0.0 | 0.0 | 0.1 | 1.3 | 6.5 | 0.5 | 1.6 |
| bench_pubsub_1sub_tcp | - | 10000 | 512 | 10453 | 956608 | 0.0 | 0.0 | 0.1 | 1.3 | 3.1 | 0.3 | 3.4 |
| bench_pubsub_1sub_unix | - | 10000 | 512 | 8589 | 1164220 | 0.0 | 0.0 | 0.1 | 1.2 | 6.4 | 0.5 | 1.6 |
| bench_pubsub_2sub_tcp | sub0 | 5000 | 256 | 19991 | 250101 | 0.0 | 0.0 | 0.0 | 0.1 | 4.5 | 0.5 | 13.1 |
| bench_pubsub_2sub_tcp | sub1 | 4114 | 256 | 19991 | 205783 | 0.0 | 0.0 | 0.0 | 0.1 | 4.8 | 0.4 | 9.6 |
| bench_pubsub_2sub_tcp | total | 9114 | 256 | 19991 | 455885 | 0.0 | 0.0 | 0.0 | 0.1 | 4.7 | 0.4 | 11.6 |
| bench_pubsub_2sub_unix | sub0 | 5000 | 256 | 4192 | 1192696 | 0.0 | 0.0 | 0.1 | 0.1 | 14.2 | 0.6 | 2.7 |
| bench_pubsub_2sub_unix | sub1 | 5000 | 256 | 4192 | 1192696 | 0.0 | 0.0 | 0.1 | 0.1 | 14.4 | 0.6 | 2.7 |
| bench_pubsub_2sub_unix | total | 10000 | 256 | 4192 | 2385393 | 0.0 | 0.0 | 0.1 | 0.1 | 14.4 | 0.6 | 2.7 |
| bench_reqrep_tcp | - | 5000 | 128 | 145158 | 34445 | 25.1 | 27.6 | 27.9 | 32.2 | 49.5 | 29.0 | 4.5 |
| bench_reqrep_unix | - | 5000 | 128 | 33096 | 151075 | 6.2 | 6.4 | 6.5 | 6.6 | 9.3 | 6.6 | 0.7 |
| bench_sizes_pair_tcp | - | 2000 | 64 | 9960 | 200800 | 0.0 | 0.0 | 0.0 | 0.0 | 3.4 | 5.0 | 77.5 |
| bench_sizes_pair_tcp | - | 2000 | 1024 | 11020 | 181486 | 0.0 | 0.1 | 1.7 | 1.9 | 4.0 | 5.5 | 78.4 |
| bench_sizes_pair_tcp | - | 2000 | 65536 | 82963 | 24106 | 19.3 | 20.6 | 23.1 | 28.5 | 358.3 | 41.5 | 319.0 |
| bench_sizes_pair_unix | - | 2000 | 64 | 1671 | 1196835 | 0.0 | 0.0 | 0.1 | 0.1 | 6.5 | 0.8 | 11.2 |
| bench_sizes_pair_unix | - | 2000 | 1024 | 2689 | 743640 | 0.0 | 0.1 | 1.2 | 6.1 | 9.5 | 1.3 | 4.0 |
| bench_sizes_pair_unix | - | 2000 | 65536 | 152637 | 13102 | 59.8 | 62.5 | 62.7 | 64.3 | 76.6 | 76.3 | 428.1 |
| bench_pushpull_throughput_tcp | - | 10000 | 512 | 9657 | 1035513 | 0.0 | 0.0 | 0.0 | 1.2 | 3.4 | 0.3 | 3.9 |
| bench_pushpull_throughput_unix | - | 10000 | 512 | 7129 | 1402544 | 0.0 | 0.0 | 0.1 | 1.3 | 6.8 | 0.5 | 1.6 |
| bench_pushpull_1worker_tcp | - | 10000 | 128 | 4782 | 2090747 | 0.0 | 0.0 | 0.0 | 0.0 | 1.4 | 0.1 | 2.2 |
| bench_pushpull_1worker_unix | - | 10000 | 128 | 2591 | 3858889 | 0.0 | 0.0 | 0.0 | 0.0 | 6.2 | 0.2 | 0.8 |
| bench_pushpull_3workers_tcp | worker0 | 1667 | 128 | 8859 | 188153 | 0.0 | 0.0 | 0.0 | 0.0 | 4.1 | 0.4 | 9.1 |
| bench_pushpull_3workers_tcp | worker1 | 1667 | 128 | 8859 | 188153 | 0.0 | 0.0 | 0.0 | 0.0 | 3.7 | 0.3 | 8.2 |
| bench_pushpull_3workers_tcp | worker2 | 1666 | 128 | 8859 | 188040 | 0.0 | 0.0 | 0.0 | 0.0 | 3.6 | 0.3 | 7.5 |
| bench_pushpull_3workers_tcp | total | 5000 | 128 | 8859 | 564347 | 0.0 | 0.0 | 0.0 | 0.0 | 4.8 | 0.3 | 8.3 |
| bench_pushpull_3workers_unix | worker0 | 1667 | 128 | 1650 | 1010184 | 0.0 | 0.0 | 0.1 | 0.1 | 22.7 | 0.5 | 3.2 |
| bench_pushpull_3workers_unix | worker1 | 1667 | 128 | 1650 | 1010184 | 0.0 | 0.0 | 0.1 | 0.1 | 22.9 | 0.5 | 3.2 |
| bench_pushpull_3workers_unix | worker2 | 1666 | 128 | 1650 | 1009578 | 0.0 | 0.0 | 0.1 | 0.1 | 22.8 | 0.5 | 3.2 |
| bench_pushpull_3workers_unix | total | 5000 | 128 | 1650 | 3029946 | 0.0 | 0.0 | 0.1 | 0.1 | 22.8 | 0.5 | 3.2 |

**Key observations:**
- **Unix domain sockets win big on latency**: round-trip benchmarks are ~4.4× faster on Unix (pair_latency avg 6.1μs vs 27.4μs; reqrep 6.6μs vs 29.0μs) with tighter jitter.
- **Bulk 64 KiB transfers favor TCP under backpressure**: with ACK-windowed flow control, 65536B pair delivery reaches ~24.1k msgs/s over TCP vs ~13.1k/s over Unix (1.8×; avg inter-arrival gap 41.5μs vs 76.3μs).
- **Small-message floods saturate around 650k–1.4M msgs/s** per connection for 512B payloads; PUSH/PULL with 128B frames to a single worker exceeds 2M msgs/s on TCP and 3.8M msgs/s on Unix.
- **Multi-worker PUSH/PULL divides fairly but adds per-worker overhead**: 3 workers aggregate ~564k msgs/s on TCP vs 3.0M msgs/s on Unix.
- **Pub/sub fan-out is balanced on Unix, noisy on TCP**: `pubsub_2sub` delivers 5000/5000 per subscriber on Unix; TCP fan-out varies run to run (5000/4114 here).
- **Backpressure-safe by construction**: flood volumes beyond powpow's `maxWriteBufferSize` cap (default 32MB, tunable via `-d:maxWriteBufferSize=N` in MB) are handled by ACK windows + watchdog timers instead of silent connection death.

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/whiz/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/whiz/fork)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
