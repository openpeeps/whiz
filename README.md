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

```
[Suite] bench_zmtp
  bench_pair_latency  N=5000  size=1  98626us  50696/s
  [OK] bench_pair_latency
  bench_pair_throughput  N=10000  size=512  23014us  434518/s
  [OK] bench_pair_throughput
  bench_pubsub_1sub  N=10000  size=512  19494us  512978/s
  [OK] bench_pubsub_1sub
  bench_pubsub_2sub  N=5000  size=256  14203us  704076/s
  [OK] bench_pubsub_2sub
  bench_reqrep  N=5000  size=128  143962us  34731/s
  [OK] bench_reqrep
  bench_sizes_pair  N=2000  size=64  6377us  313627/s
  bench_sizes_pair  N=2000  size=1024  8975us  222841/s
  bench_sizes_pair  N=2000  size=65536  575434us  3475/s
  [OK] bench_sizes_pair
  bench_pushpull_throughput  N=10000  size=512  110386us  90591/s
  [OK] bench_pushpull_throughput
  bench_pushpull_1worker  N=10000  size=128  11950us  836820/s
  [OK] bench_pushpull_1worker
  bench_pushpull_3workers  N=5000  size=128  8454us  591436/s
  [OK] bench_pushpull_3workers
```

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/whiz/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/whiz/fork)

|  |  |
|---|---|
| <a href="https://opencode.ai/go?ref=BHMEEK48QX"><img src="https://github.com/openpeeps/whiz/blob/main/.github/opencode.png" alt="OpenCode"></a> | Switch to **Open-Source LLMs** via OpenCode GO, choosing from a variety of powerful models such as DeepSeek, Qwen, Kimi, GLM-5, MiniMax, MiMo. 🍕 [Use our referral link to get started!](https://opencode.ai/go?ref=BHMEEK48QX)|

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
