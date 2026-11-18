# Package

version       = "0.1.0"
author        = "OpenPeeps"
description   = "A message queue library implementing ZMTP 3.0 in Nim"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.2.10"
requires "powpow >= 0.2.7"
requires "nimcypher >= 0.4.0"

task bench, "Run all benchmarks":
  exec "nim c -d:release -r tests/bench_zmtp.nim"
