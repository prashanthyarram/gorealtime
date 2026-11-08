# Milestone 6 — performance

## Proposed design

Measure the local path and write the numbers down. Do not change locking, allocation, or goroutine use unless a run shows a concrete cost, and then measure that change again.

Benchmarks use an in-memory session whose `Write` appends the frame. They do not include a TCP round trip or `coder/websocket` framing. That keeps the sample on registry, JSON, queue, and fan-out. A separate stress test dials real sockets through `httptest` so disconnect still joins the network stack and the transport's goroutines.

Heartbeats are off in these runs. A ping timer would make a short benchmark look uneven.

What is timed:

- register and remove one connection on the local registry
- `handleFrame` for one valid `ping` event with a handler that returns nil
- `Send` of one small JSON object to one connection
- `Broadcast` to 32 connections
- `SendToGroup` to those same 32 after they join one group
- `Broadcast` from `RunParallel` to 16 connections

The stress test opens 16 real clients, broadcasts 20 times, shuts down, and checks that goroutines come back to the baseline. `GOREALTIME_STRESS=1` raises that to 100 clients and 50 broadcasts. No throughput target. The check is cleanup.

## Package responsibilities

- `benchmark_test.go` and `stress_test.go` only.
- `docs/performance.md` records the command, the machine, and the tool output. Numbers in that file come from the run, not from a guess.

## Trade-offs

- An in-memory write is faster than a socket, so the benchmark overstates how many broadcasts a process can push to real clients. The document says so.
- `b.N` for a broadcast is one fan-out, not one recipient.
- No profile-guided rewrite in this milestone unless the first run shows a cost that is both large and local to a few lines.

## Concurrency

The parallel broadcast shares one server. A full queue retries until the write goroutine takes a frame. The sample is the sustainable rate, including that wait. Any other error fails the run. Slow-consumer close is disabled so a saturated second does not drop the fixture.

## Security

Benchmarks do not log frames. The stress test does not print payloads.

## Public API

None.

## Tests

`go test -bench=. -benchmem -count=3 -run='^$'` from the module root. `go test ./...` runs the modest stress test. The heavier loop stays behind `GOREALTIME_STRESS`.
