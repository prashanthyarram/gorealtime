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

No library code was changed after this run.

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
