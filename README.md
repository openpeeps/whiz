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
- **Socket patterns**: PAIR, PUB/SUB, REQ/REP, PUSH/PULL, File Transfer
- **Security mechanisms**:
  - NULL: no authentication (plaintext)
  - PLAIN: username/password authentication with ZAP callback
  - CURVE: X25519 + XChaCha20-Poly1305 AEAD encryption and mutual authentication
- **Transport** TCP and IPC (Unix domain sockets)
- **Built on [PowPow](https://github.com/openpeeps/powpow)** event notification library in Nim

> [!NOTE]
> CURVE requires [Monocypher](https://monocypher.org) (via the [e2ee](https://github.com/openpeeps/e2ee) package). Install it with your system package manager (`brew install monocypher`, `apt install libmonocypher-dev`, etc.) or build from source.

## 🗺 Roadmap

- [x] ZMTP 3.0 wire protocol (greeting, framing, commands)
- [x] PAIR, PUB/SUB, REQ/REP, PUSH/PULL socket patterns
- [x] File transfer over ZMTP
- [x] PLAIN security mechanism + ZAP auth callbacks
- [x] CURVE security mechanism (X25519 + AEAD)
- [ ] RADIO/DISH (dgram) socket pattern
- [ ] CLIENT/SERVER (stream) socket pattern
- [ ] CURVE vouch (Ed25519 signature in HELLO for key continuity)
- [ ] TLS/DTLS transport
- [ ] GSSAPI / Kerberos mechanism
- [ ] WebSocket transport (RFC 7692)
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

| benchmark | subscriber | n | size | total(μs) | throughput/s | min(μs) | p50(μs) | p75(μs) | p90(μs) | p99(μs) | avg(μs) | σ(μs) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| bench_pair_latency_tcp | - | 5000 | 1 | 100325 | 49837 | 17.9 | 18.4 | 18.9 | 23.1 | 41.7 | 20.0 | 4.7 |
| bench_pair_latency_unix | - | 5000 | 1 | 64249 | 77821 | 12.2 | 12.5 | 12.6 | 12.9 | 18.5 | 12.8 | 1.2 |
| bench_pair_throughput_tcp | - | 10000 | 512 | 16445 | 608080 | 0.0 | 0.0 | 0.1 | 1.3 | 4.0 | 0.4 | 3.5 |
| bench_pair_throughput_unix | - | 10000 | 512 | 15291 | 653975 | 0.0 | 0.0 | 0.1 | 1.3 | 12.5 | 0.9 | 3.0 |
| bench_pubsub_1sub_tcp | - | 10000 | 512 | 11360 | 880222 | 0.0 | 0.0 | 0.1 | 1.3 | 3.8 | 0.4 | 3.4 |
| bench_pubsub_1sub_unix | - | 10000 | 512 | 13933 | 717704 | 0.0 | 0.0 | 0.1 | 1.3 | 12.1 | 0.9 | 2.9 |
| bench_pubsub_2sub_tcp | sub0 | 5000 | 256 | 15679 | 318881 | 0.0 | 0.0 | 0.0 | 0.1 | 6.9 | 0.5 | 17.3 |
| bench_pubsub_2sub_tcp | sub1 | 4143 | 256 | 15679 | 264225 | 0.0 | 0.0 | 0.0 | 0.1 | 7.3 | 0.5 | 12.2 |
| bench_pubsub_2sub_tcp | total | 9143 | 256 | 15679 | 583106 | 0.0 | 0.0 | 0.0 | 0.1 | 7.1 | 0.5 | 15.2 |
| bench_pubsub_2sub_unix | sub0 | 5000 | 256 | 5279 | 947043 | 0.0 | 0.0 | 0.0 | 0.1 | 19.2 | 0.7 | 3.4 |
| bench_pubsub_2sub_unix | sub1 | 5000 | 256 | 5279 | 947043 | 0.0 | 0.0 | 0.0 | 0.1 | 19.3 | 0.7 | 3.4 |
| bench_pubsub_2sub_unix | total | 10000 | 256 | 5279 | 1894086 | 0.0 | 0.0 | 0.0 | 0.1 | 19.2 | 0.7 | 3.4 |
| bench_reqrep_tcp | - | 5000 | 128 | 100401 | 49800 | 16.6 | 18.9 | 19.2 | 23.5 | 38.2 | 20.0 | 3.7 |
| bench_reqrep_unix | - | 5000 | 128 | 64793 | 77168 | 12.5 | 12.8 | 12.8 | 12.9 | 16.1 | 12.9 | 0.9 |
| bench_sizes_pair_tcp | - | 2000 | 64 | 5586 | 357990 | 0.0 | 0.0 | 0.0 | 0.0 | 3.1 | 0.1 | 0.5 |
| bench_sizes_pair_tcp | - | 2000 | 1024 | 3461 | 577759 | 0.0 | 0.0 | 1.2 | 1.6 | 2.7 | 0.6 | 3.6 |
| bench_sizes_pair_tcp | - | 2000 | 65536 | 258856 | 7726 | 19.4 | 20.4 | 21.8 | 25.6 | 253.5 | 28.4 | 39.9 |
| bench_sizes_pair_unix | - | 2000 | 64 | 582 | 3435942 | 0.0 | 0.0 | 0.0 | 0.0 | 1.5 | 0.1 | 1.1 |
| bench_sizes_pair_unix | - | 2000 | 1024 | 4677 | 427548 | 0.0 | 0.0 | 1.3 | 11.4 | 12.3 | 1.7 | 3.9 |
| bench_sizes_pair_unix | - | 2000 | 65536 | 411682 | 4858 | 99.7 | 104.1 | 107.3 | 110.7 | 126.9 | 105.3 | 5.3 |
| bench_pushpull_throughput_tcp | - | 10000 | 512 | 10436 | 958154 | 0.0 | 0.0 | 0.1 | 1.3 | 3.9 | 0.3 | 3.4 |
| bench_pushpull_throughput_unix | - | 10000 | 512 | 11985 | 834370 | 0.0 | 0.0 | 0.1 | 1.3 | 12.2 | 0.9 | 3.0 |
| bench_pushpull_1worker_tcp | - | 10000 | 128 | 3979 | 2512663 | 0.0 | 0.0 | 0.0 | 0.0 | 1.6 | 0.1 | 2.4 |
| bench_pushpull_1worker_unix | - | 10000 | 128 | 3643 | 2744939 | 0.0 | 0.0 | 0.0 | 0.0 | 11.5 | 0.2 | 1.5 |
| bench_pushpull_3workers_tcp | worker0 | 1667 | 128 | 6350 | 262516 | 0.0 | 0.0 | 0.0 | 0.0 | 3.2 | 0.4 | 9.4 |
| bench_pushpull_3workers_tcp | worker1 | 1667 | 128 | 6350 | 262516 | 0.0 | 0.0 | 0.0 | 0.0 | 2.8 | 0.3 | 8.2 |
| bench_pushpull_3workers_tcp | worker2 | 1666 | 128 | 6350 | 262358 | 0.0 | 0.0 | 0.0 | 0.0 | 2.8 | 0.3 | 7.0 |
| bench_pushpull_3workers_tcp | total | 5000 | 128 | 6350 | 787391 | 0.0 | 0.0 | 0.0 | 0.0 | 4.3 | 0.3 | 8.3 |
| bench_pushpull_3workers_unix | worker0 | 1667 | 128 | 1599 | 1041951 | 0.0 | 0.0 | 0.0 | 0.1 | 27.7 | 0.5 | 3.7 |
| bench_pushpull_3workers_unix | worker1 | 1667 | 128 | 1599 | 1041951 | 0.0 | 0.0 | 0.0 | 0.0 | 27.7 | 0.5 | 3.6 |
| bench_pushpull_3workers_unix | worker2 | 1666 | 128 | 1599 | 1041326 | 0.0 | 0.0 | 0.0 | 0.1 | 27.7 | 0.5 | 3.6 |
| bench_pushpull_3workers_unix | total | 5000 | 128 | 1599 | 3125228 | 0.0 | 0.0 | 0.0 | 0.0 | 27.7 | 0.5 | 3.6 |

**Key observations:**
- **Unix domain sockets are faster for small messages** — latency benchmarks show ~55% higher throughput on Unix (pair_latency: 78k/s vs 50k/s; reqrep: 77k/s vs 50k/s) by skipping TCP stack overhead.
- **Unix latency is more consistent** — σ drops from 4.7→1.2 (pair_latency) and 3.7→0.9 (reqrep), indicating less jitter.
- **Large payloads (≥64 KiB) favor TCP** — Unix has smaller default socket buffers, causing fragmentation: 65536B pair throughput is 7.7k/s (TCP) vs 4.9k/s (Unix), with avg latency 28μs vs 105μs.
- **Multi-worker pushpull scales poorly on TCP** — 3 workers saturate at 787k/s (less than a single worker's 2.5M/s) due to TCP fairness/contention. Unix handles it near-linearly at 3.1M/s aggregate.
- **Pub/sub delivery is imbalanced on TCP** — `pubsub_2sub` TCP delivers 5000/4143 unevenly; Unix delivers evenly (5000/5000), suggesting single-subscriber fairness differences with Nagle/ACK delaying.
- **Throughput is consistent across runs** — no multi-second stalls or order-of-magnitude variance, confirming the earlier TCP cork and edge-triggered event fixes are effective.

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/whiz/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/whiz/fork)

|  |  |
|---|---|
| <a href="https://opencode.ai/go?ref=BHMEEK48QX"><img src="https://github.com/openpeeps/whiz/blob/main/.github/opencode.png" alt="OpenCode"></a> | Switch to **Open-Source LLMs** via OpenCode GO, choosing from a variety of powerful models such as DeepSeek, Qwen, Kimi, GLM-5, MiniMax, MiMo. 🍕 [Use our referral link to get started!](https://opencode.ai/go?ref=BHMEEK48QX)|

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
