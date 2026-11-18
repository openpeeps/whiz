# Package

version       = "0.1.0"
author        = "OpenPeeps"
description   = "A message queue library implementing ZMTP 3.0 in Nim"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.4.0"
requires "powpow >= 0.2.7"
requires "nimcypher >= 0.4.0"
requires "buffalo >= 0.1.0"
requires "zmq >= 1.5.3" # bench_vs_zmq only (needs C libzmq at runtime)

task bench, "Run all benchmarks":
  exec "nim c -d:release -r tests/bench_zmtp.nim"

task benchMem, "Run memory benchmarks":
  exec "nim c -d:release -r tests/bench_mem.nim"

task benchVsZmq, "Run whiz vs ØMQ benchmarks (needs libzmq at runtime)":
  exec "nim c -d:release -r tests/bench_vs_zmq.nim"
