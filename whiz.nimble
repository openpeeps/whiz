# Package

version       = "0.1.0"
author        = "OpenPeeps"
description   = "A message queue library implementing ZMTP 3.0 in Nim"
license       = "MIT"
srcDir        = "src"


# Dependencies

requires "nim >= 2.2.0"
requires "powpow >= 0.1.4"
requires "e2ee >= 0.1.0"


# Tests (run `nimble test --noinstall` to skip external dependency resolution)

task test, "Run all unit tests":
  exec "nim c -r tests/test_pair.nim"
  exec "nim c -r tests/test_pubsub.nim"
  exec "nim c -r tests/test_reqrep.nim"
  exec "nim c -r tests/test_filetx.nim"
  exec "nim c -r tests/test_pushpull.nim"
  exec "nim c -r tests/test_auth.nim"
  exec "nim c -r tests/test_curve.nim"

task bench, "Run all benchmarks":
  exec "nim c -d:release -r tests/bench_zmtp.nim"
