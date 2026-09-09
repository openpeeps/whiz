<p align="center">
  <img src="https://github.com/openpeeps/PKG/blob/main/.github/logo.png" width="90px"><br>
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
- [ ] CURVE vouch (Ed25519 signature in HELLO for key continuity)
- [ ] TLS/DTLS transport
- [ ] GSSAPI / Kerberos mechanism
- [x] WebSocket transport (RFC 6455)
- [ ] Formal ZMTP conformance tests
- [ ] Performance benchmarks vs ØMQ


## Examples
...

### Benchmarks
> [!NOTE]
> Benchmark results are not consistent and may show different results across runs and environments.

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
| bench_pair_latency_tcp | - | 5000 | 1 | 413222 | 12100 | 73.7 | 76.1 | 80.9 | 100.0 | 145.3 | 82.5 | 15.0 |
| bench_pair_latency_unix | - | 5000 | 1 | 372268 | 13431 | 67.8 | 68.4 | 73.6 | 89.5 | 125.7 | 74.3 | 12.7 |
| bench_pair_throughput_tcp | - | 10000 | 512 | 27907 | 358332 | 0.2 | 0.2 | 0.2 | 2.0 | 5.0 | 0.7 | 6.1 |
| bench_pair_throughput_unix | - | 10000 | 512 | 64441 | 155179 | 0.2 | 0.2 | 0.2 | 1.9 | 82.5 | 4.9 | 17.6 |
| bench_pubsub_1sub_tcp | - | 10000 | 512 | 23566 | 424336 | 0.2 | 0.2 | 0.3 | 2.0 | 4.8 | 0.7 | 6.2 |
| bench_pubsub_1sub_unix | - | 10000 | 512 | 22232 | 449790 | 0.2 | 0.2 | 0.2 | 1.8 | 10.1 | 0.9 | 2.5 |
| bench_pubsub_2sub_tcp | sub0 | 5000 | 256 | 23489 | 212864 | 0.2 | 0.2 | 0.2 | 0.2 | 7.8 | 0.8 | 24.1 |
| bench_pubsub_2sub_tcp | sub1 | 4143 | 256 | 23489 | 176379 | 0.2 | 0.2 | 0.2 | 0.2 | 7.8 | 0.8 | 20.5 |
| bench_pubsub_2sub_tcp | total | 9143 | 256 | 23489 | 389244 | 0.2 | 0.2 | 0.2 | 0.2 | 7.8 | 0.8 | 22.5 |
| bench_pubsub_2sub_unix | sub0 | 5000 | 256 | 14531 | 344082 | 0.2 | 0.2 | 0.3 | 0.3 | 35.0 | 1.3 | 5.7 |
| bench_pubsub_2sub_unix | sub1 | 5000 | 256 | 14531 | 344082 | 0.2 | 0.2 | 0.3 | 0.3 | 35.0 | 1.3 | 5.7 |
| bench_pubsub_2sub_unix | total | 10000 | 256 | 14531 | 688164 | 0.2 | 0.2 | 0.3 | 0.3 | 35.0 | 1.3 | 5.7 |
| bench_reqrep_tcp | - | 5000 | 128 | 450034 | 11110 | 73.7 | 78.5 | 96.3 | 120.5 | 164.8 | 89.8 | 21.4 |
| bench_reqrep_unix | - | 5000 | 128 | 367558 | 13603 | 68.0 | 68.6 | 71.5 | 86.8 | 116.7 | 73.4 | 10.5 |
| bench_sizes_pair_tcp | - | 2000 | 64 | 7525 | 265775 | 0.1 | 0.2 | 0.2 | 0.2 | 4.0 | 3.8 | 56.6 |
| bench_sizes_pair_tcp | - | 2000 | 1024 | 9360 | 213652 | 0.2 | 0.2 | 2.3 | 2.7 | 5.0 | 4.7 | 60.0 |
| bench_sizes_pair_tcp | - | 2000 | 65536 | 282384 | 7082 | 24.5 | 28.1 | 33.2 | 44.8 | 351.2 | 141.2 | 1584.9 |
| bench_sizes_pair_unix | - | 2000 | 64 | 4612 | 433560 | 0.1 | 0.2 | 0.2 | 0.3 | 70.6 | 2.3 | 23.0 |
| bench_sizes_pair_unix | - | 2000 | 1024 | 23033 | 86830 | 0.2 | 0.2 | 1.8 | 66.0 | 91.0 | 11.5 | 42.2 |
| bench_sizes_pair_unix | - | 2000 | 65536 | 1372650 | 1457 | 538.6 | 550.1 | 594.5 | 665.8 | 826.3 | 686.3 | 1691.0 |
| bench_pushpull_throughput_tcp | - | 10000 | 512 | 19621 | 509656 | 0.2 | 0.2 | 0.2 | 1.9 | 5.0 | 0.6 | 5.7 |
| bench_pushpull_throughput_unix | - | 10000 | 512 | 60745 | 164620 | 0.2 | 0.2 | 0.2 | 1.9 | 82.0 | 4.9 | 17.8 |
| bench_pushpull_1worker_tcp | - | 10000 | 128 | 9022 | 1108394 | 0.1 | 0.2 | 0.2 | 0.2 | 2.2 | 0.3 | 3.2 |
| bench_pushpull_1worker_unix | - | 10000 | 128 | 7878 | 1269208 | 0.1 | 0.2 | 0.2 | 0.2 | 8.8 | 0.4 | 1.3 |
| bench_pushpull_3workers_tcp | worker0 | 1667 | 128 | 10084 | 165302 | 0.2 | 0.2 | 0.2 | 0.2 | 8.2 | 0.8 | 17.4 |
| bench_pushpull_3workers_tcp | worker1 | 1667 | 128 | 10084 | 165302 | 0.2 | 0.2 | 0.2 | 0.2 | 4.2 | 0.8 | 20.7 |
| bench_pushpull_3workers_tcp | worker2 | 1666 | 128 | 10084 | 165203 | 0.2 | 0.2 | 0.2 | 0.2 | 4.1 | 0.9 | 22.0 |
| bench_pushpull_3workers_tcp | total | 5000 | 128 | 10084 | 495807 | 0.2 | 0.2 | 0.2 | 0.2 | 7.6 | 0.8 | 20.1 |
| bench_pushpull_3workers_unix | worker0 | 1667 | 128 | 4079 | 408632 | 0.2 | 0.2 | 0.2 | 0.2 | 46.7 | 1.0 | 6.5 |
| bench_pushpull_3workers_unix | worker1 | 1667 | 128 | 4079 | 408632 | 0.2 | 0.2 | 0.2 | 0.2 | 46.6 | 1.0 | 6.3 |
| bench_pushpull_3workers_unix | worker2 | 1666 | 128 | 4079 | 408386 | 0.2 | 0.2 | 0.2 | 0.2 | 46.8 | 1.0 | 6.1 |
| bench_pushpull_3workers_unix | total | 5000 | 128 | 4079 | 1225650 | 0.2 | 0.2 | 0.2 | 0.2 | 46.8 | 1.0 | 6.3 |

**Key observations:**
- **Unix domain sockets win on latency** — round-trip benchmarks are ~12% faster on Unix (pair_latency avg 74μs vs 83μs; reqrep 73μs vs 90μs) with tighter jitter.
- **Bulk 64 KiB transfers strongly favor TCP under backpressure** — with ACK-windowed flow control, 65536B pair delivery reaches ~7.1k msgs/s over TCP vs ~1.5k/s over Unix (4.9×): smaller Unix-socket buffers fragment large writes into many more wakeups (avg inter-arrival gap 686μs vs 141μs).
- **Small-message floods saturate around 350–500k msgs/s** per connection for 512B payloads; PUSH/PULL with 128B frames to a single worker exceeds 1.1M msgs/s on both transports.
- **Multi-worker PUSH/PULL divides fairly but adds per-worker overhead** — 3 workers aggregate ~496k msgs/s on TCP vs 1.2M msgs/s on Unix.
- **Pub/sub fan-out stays imbalanced on TCP** — `pubsub_2sub` delivers 5000/4143 across subscribers before the first completes the run; Unix fans out evenly (5000/5000).
- **Backpressure-safe by construction** — flood volumes beyond powpow's `maxWriteBufferSize` cap (default 32MB, tunable via `-d:maxWriteBufferSize=N` in MB) are handled by ACK windows + watchdog timers instead of silent connection death.

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/whiz/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/whiz/fork)

|  |  |
|---|---|
| <a href="https://opencode.ai/go?ref=BHMEEK48QX"><img src="https://github.com/openpeeps/whiz/blob/main/.github/opencode.png" alt="OpenCode"></a> | Switch to **Open-Source LLMs** via OpenCode GO, choosing from a variety of powerful models such as DeepSeek, Qwen, Kimi, GLM-5, MiniMax, MiMo. 🍕 [Use our referral link to get started!](https://opencode.ai/go?ref=BHMEEK48QX)|

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
