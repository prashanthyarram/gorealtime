# Performance

These numbers are one run on one machine. They are not a capacity guarantee and they are not a comparison with a socket.

## Environment

- Go `go1.22.2 linux/amd64`
- Kernel `Linux 6.12.113+ x86_64`
- CPU `Intel(R) Xeon(R) Processor`, 4 cores, 2400 MHz
- `GOMAXPROCS` 4, taken from the `-4` suffix on the benchmark names. `go env GOMAXPROCS` was empty, so the process used the default.
- Command, from the module root:

```text
go test -bench=. -benchmem -count=3 -run='^$' .
```

The tool printed `cpu: Intel(R) Xeon(R) Processor`.

## What was timed

The session is in memory. `Write` adds the frame length to a counter and drops the bytes. There is no TCP connection and no WebSocket framing. Heartbeats are off. The slow-consumer close is off. Each queue holds 4096 frames.

`BenchmarkRegistryAddRemove` includes a `crypto/rand` id.

`BenchmarkDispatch` is one valid `ping` frame and a handler that returns nil.

`BenchmarkSendOne`, `BenchmarkBroadcast32`, `BenchmarkGroupBroadcast32`, and `BenchmarkParallelBroadcast16` call the public send API. If the queue returns `ErrQueueFull`, the harness retries until the call returns nil. `ns/op` is that wait included. `allocs/op` counts every attempt, including attempts the queue rejected. `b.N` for a broadcast is one fan-out, not one recipient. The group benchmark is the same 32 connections after `Join("room")`.

`SendOne` stayed at 14 allocs/op across three samples, which fits a writer that accepted the frame on the first attempt. The broadcast samples are larger. This file does not split snapshot cost from retried encodes.

No library code was changed between that run and the table below. Compression was added later and is off unless `WithCompression` sets `Enabled`. The benchmarks in this section do not deflate.

## Results

```text
BenchmarkRegistryAddRemove-4       613957    1767 ns/op    1160 B/op    16 allocs/op
BenchmarkRegistryAddRemove-4       647670    1788 ns/op    1160 B/op    16 allocs/op
BenchmarkRegistryAddRemove-4       621862    1776 ns/op    1160 B/op    16 allocs/op
BenchmarkDispatch-4                750270    1525 ns/op     392 B/op    11 allocs/op
BenchmarkDispatch-4                721648    1519 ns/op     392 B/op    11 allocs/op
BenchmarkDispatch-4                790018    1521 ns/op     392 B/op    11 allocs/op
BenchmarkSendOne-4                 764222    1453 ns/op     720 B/op    14 allocs/op
BenchmarkSendOne-4                 795321    1465 ns/op     720 B/op    14 allocs/op
BenchmarkSendOne-4                 807060    1472 ns/op     720 B/op    14 allocs/op
BenchmarkBroadcast32-4             160879    7816 ns/op    8408 B/op   138 allocs/op
BenchmarkBroadcast32-4             163864    7766 ns/op    8409 B/op   138 allocs/op
BenchmarkBroadcast32-4             171492    7592 ns/op    8415 B/op   138 allocs/op
BenchmarkGroupBroadcast32-4        163309    7642 ns/op    8411 B/op   138 allocs/op
BenchmarkGroupBroadcast32-4        135249    7742 ns/op    8406 B/op   138 allocs/op
BenchmarkGroupBroadcast32-4        165357    7360 ns/op    8405 B/op   138 allocs/op
BenchmarkParallelBroadcast16-4     260751    4575 ns/op    4446 B/op    74 allocs/op
BenchmarkParallelBroadcast16-4     254022    4504 ns/op    4450 B/op    75 allocs/op
BenchmarkParallelBroadcast16-4     270634    4471 ns/op    4450 B/op    75 allocs/op
```

Derived from the second sample only, as `1e9 / ns`:

| Benchmark | Second sample | Operations per second |
| --- | --- | --- |
| Registry add and remove | 1788 ns | 559,000 |
| Dispatch one event | 1519 ns | 658,000 |
| Send one connection | 1465 ns | 683,000 |
| Broadcast to 32 | 7766 ns | 129,000 fan-outs |
| Group broadcast to 32 | 7742 ns | 129,000 fan-outs |
| Parallel broadcast to 16 | 4504 ns | 222,000 fan-outs |

The per-second column is rounded to the nearest thousand. The `ns/op` column is the measurement.

## Stress

`TestStressFanout` dials 16 real clients through `httptest`, broadcasts 20 events, shuts the server down, and fails if goroutines stay more than 8 above the baseline. `go test ./...` ran that test in this milestone and it passed. `GOREALTIME_STRESS=1` uses 100 clients and 50 broadcasts. That setting was not used for the table above.

## Compression

These numbers are a later run on the same class of machine. They time `compress/flate` and a real WebSocket echo. They are not a claim that compression makes the server faster. A repetitive payload shrinks a lot and costs CPU. Random bytes do not shrink when each message starts from an empty window, and the attempt is still expensive.

### Environment

- Go `go1.22.2 linux/amd64`
- Kernel `Linux 6.12.113+ x86_64`
- CPU `Intel(R) Xeon(R) Processor`, 4 cores. `lscpu` did not print a frequency.
- `GOMAXPROCS` 4, from the `-4` suffix. The tool printed `cpu: Intel(R) Xeon(R) Processor`.
- Command, from the module root:

```text
go test -bench=. -benchmem -count=3 -run='^$' ./internal/ws/
```

`BenchmarkCompress` and `BenchmarkDecompress` call the per-connection deflate state directly. Level is 6. `wire-B` is the compressed size of the payload after one warmup call, so a context-takeover case is the second copy of the same buffer, not an independent message. `ratio` is uncompressed size divided by `wire-B`.

`BenchmarkEcho` is one client and one server on `httptest`, writing a message and reading the echo. `off-*` does not negotiate compression. `on-*` does, with `MinSize` 1, so a 4-byte payload is compressed too. `BenchmarkEchoParallel` is the same echo with compression on, from `RunParallel` workers. The handshake sits inside each worker before the timed loop.

Payloads:

| Name | Bytes | Contents |
| --- | --- | --- |
| small-skip | 4 | `ping`, minimum size 128, so the compressor is not called |
| small-force / small | 16 | `a` repeated, minimum size 1 |
| text | 49152 | `hello world ` repeated. The echo text case is 3072 bytes. Parallel is 768 bytes. |
| random | 8192 | `math/rand` seed 1. The echo random case is 4096 bytes. |

### Results

Second sample of three. `ns/op` is the measurement. The ratio column is the same on every sample.

| Benchmark | ns/op | B/op | allocs/op | wire bytes | ratio |
| --- | --- | --- | --- | --- | --- |
| Compress small-skip | 1.529 | 0 | 0 | 4 | 1.0 |
| Compress small-force | 11282 | 0 | 0 | 5 | 3.2 |
| Compress text | 103618 | 0 | 0 | 127 | 387 |
| Compress text, context takeover | 87806 | 0 | 0 | 112 | 439 |
| Compress random | 78945 | 0 | 0 | 8192 | 1.0 |
| Compress random, same buffer again with context | 17019 | 0 | 0 | 69 | 119 |
| Decompress text | 43406 | 183089 | 10 | 127 | 387 |
| Decompress text, context takeover | 45762 | 183089 | 10 | 127 | 387 |
| Decompress random, forced deflate | 4325 | 22576 | 3 | 8198 | 1.0 |
| Decompress small | 1287 | 4208 | 3 | 5 | 3.2 |
| Echo text, compression off | 14160 | 21776 | 13 |  |  |
| Echo text, compression on | 57886 | 17968 | 19 |  |  |
| Echo text, context takeover | 69598 | 181556 | 23 |  |  |
| Echo random, compression off | 18659 | 30224 | 13 |  |  |
| Echo random, compression on | 143933 | 30227 | 13 |  |  |
| Echo 4-byte payload, compression on | 31821 | 64 | 11 |  |  |
| Echo parallel, compression on | 15221 | 10974 | 19 |  |  |

The repetitive 49,152-byte buffer compresses to 127 bytes. Inflating it allocates about 183 KiB and takes about 43 µs in this run. The echo of a 3,072-byte repetitive buffer is about 14 µs with compression off and about 58 µs with it on.

The 8,192-byte random buffer is sent unchanged when context takeover is off. The compressor still spent about 79 µs deciding that. The echo of 4,096 random bytes is about 19 µs with compression off and about 144 µs with it on, at the same allocation count, because the compressed form was larger and the original bytes were sent. The 69-byte figure in the context row is that same random buffer repeated into a live window. It is not a ratio for two different random messages.

Context takeover on the echo also retains the window: about 182 KiB and 23 allocations per echo, against about 18 KiB and 19 allocations without it. The default is to keep takeover off.

A 4-byte payload with the minimum size forced to 1 costs about 32 µs for an echo. Compressing 16 repeated bytes, without the socket, is about 11 µs. The default minimum of 128 bytes skips that path. The skip itself is about 1.5 ns and allocates nothing.

