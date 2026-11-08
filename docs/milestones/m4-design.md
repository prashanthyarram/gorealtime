# Milestone 4 — messaging

## Proposed design

Every outbound data frame, including hello and error replies, goes through one bounded queue per connection. A single write goroutine drains that queue. Callers enqueue without waiting for the socket. If the queue cannot accept the frame, the caller gets `ErrQueueFull` immediately. The frame is not kept anywhere else.

The queue remembers when it first became saturated. A later full enqueue, after `WithSlowConsumerTimeout` (default 15s), closes the connection with `slow_consumer` and returns `ErrSlowConsumer`. A successful enqueue clears the timer. Zero timeout disables the close and only returns `ErrQueueFull`. One burst that overflows the queue does not drop the client; a client that stays wedged does.

`Send`, `SendToIdentity`, `Broadcast`, and `SendToGroup` encode the event once and enqueue that slice to each target. Server frames are not masked and are not mutated, so sharing the slice is safe. Closed connections are skipped. `ErrQueueFull` from any target is returned after the others have been attempted. `ExcludeConnections` skips ids.

Groups are process-local. Joining creates the group. Leaving and disconnect remove the member. An empty group disappears. There is no distributed membership. `AddToGroup` and `Join` are trusted server calls. An authorizer is the next milestone; this one does not pretend to know who may join.

## Package responsibilities

- `connection.go`: queue, enqueue, write goroutine, saturation timer. The socket field is a small interface so a test can block a write without filling a TCP buffer.
- `groups.go`: membership indexes.
- `send.go`: encode once, deliver to snapshots.
- `dispatch.go`: error replies enqueue instead of writing inline.

## Trade-offs

- Non-blocking enqueue, not a blocking send with a caller timeout. A blocking send would tie a broadcast to the slowest socket and hide the queue bound inside a goroutine the caller cannot see. The error is immediate and the caller decides whether to retry.
- One write goroutine, not a write per `Send`. Order per connection matches enqueue order. Hello, errors, and application events share that order.
- Encode once for a fan-out. The alternative, encoding per target, allocates N times for the same JSON. The shared slice is immutable after `Encode`.
- Saturation is timed, not counted. A count of consecutive failures disconnects during a single tight broadcast, which is a spike, not a stuck client.
- Group names use the event-name token grammar. Free-form names would allow newlines in anything that prints the name. Applications that need spaces can pick their own encoding inside the token.

## Concurrency

- The registry lock is not held while enqueueing. Snapshots are taken first.
- Group lock is separate. `finish` does not hold the connection lock while dropping groups. `Join` checks the closed flag while holding the group lock so a disconnect cannot leave a closed connection in a group.
- The write goroutine is the only caller of `Write`. `Close` still aborts the socket so a blocked write returns.
- Only the read goroutine runs `OnDisconnect` when it is in a read loop. The write goroutine signals close and exits. `Shutdown` waits for both goroutines.

## Security

- Payload bytes are still not logged. Queue-full logs the connection id only.
- A client cannot join a group by itself. Some handler the application wrote has to call `Join` or `AddToGroup`.
- Group send from server code is trusted. The chat example must check its own membership before it calls `SendToGroup`.
- Outbound memory is bounded by connections × queue length × frame size, plus the one frame currently in `Write`. It is not bounded by a global byte cap. The payload max and the queue length are the knobs.

## Public API

- `Connection.Send`, `Server.Send`, `SendToIdentity`, `Broadcast`, `SendToGroup`
- `AddToGroup`, `RemoveFromGroup`, `GroupSize`, `Connection.Join`, `Connection.Leave`
- `ExcludeConnections`, `WithEventID`, `WithCorrelationID`, `WithTrace`
- `WithQueueSize`, `WithSlowConsumerTimeout`
- `ErrQueueFull`, `ErrSlowConsumer`, `ErrConnectionNotFound`, `ErrInvalidGroup`, `ErrServerClosed`
- `ReasonSlowConsumer`
- `Info.QueueLen`, `Info.Groups`

## Tests

- A client receives `Send` and a broadcast. Two connections with one identity each receive `SendToIdentity`.
- Group join, send, exclude the sender, leave, and disconnect drops membership.
- Invalid group name, missing connection, send after shutdown.
- Raw JSON `[]byte` is not re-encoded. A struct is encoded. Invalid JSON bytes fail.
- Queue full returns `ErrQueueFull` without waiting. After the slow timeout, the next full enqueue returns `ErrSlowConsumer` and the connection closes. A successful enqueue resets the timer.
- Broadcast keeps going when one target is full and returns `ErrQueueFull`.
- Concurrent sends under the race detector.
