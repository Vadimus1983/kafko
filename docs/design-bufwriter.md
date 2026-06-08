# BufWriter for kafko -- design

> **Implementation note (shipped).** The behaviour described below as
> "Option B" was implemented, but **not** as a `std::io::BufWriter` inside
> `Segment`. It is implemented as *record accumulation in the partition writer
> task*: under `FlushPolicy::Buffered` the writer holds incoming `Record`s and
> their oneshot acks across wake-ups and issues one `Log::append_batch` (one
> `write()`) when a byte/record threshold or an idle timer fires. This achieves
> the same syscall amortisation with three advantages over a segment-level
> `BufWriter`:
>
> 1. **Offset assignment stays atomic with the write.** `Log::append_batch`
>    advances `next_offset` only after the `write()` succeeds, so a failed flush
>    burns no offsets and acks the error to every held caller — preserving the
>    existing "an IO error consumes no offset and keeps the partition alive"
>    contract. A `BufWriter` that returns from `append` before flushing would
>    split offset assignment from durability and break that contract on flush
>    failure.
> 2. **No `Read`/`Seek` conflict.** `Segment` reads and writes through one
>    `File` handle; `std::io::BufWriter<File>` implements neither `Read` nor a
>    clean read-through, so wrapping the handle would have forced a second handle
>    or manual buffer management anyway.
> 3. **One fewer copy.** Records are encoded straight into the log's reusable
>    `encode_buf` once at flush time, instead of being encoded and then copied
>    again into a `BufWriter`'s internal buffer.
>
> Config surface: `LogConfig::flush_policy: FlushPolicy`, default
> `FlushPolicy::EveryBatch` (behaviourally identical to v0.2.0). Opt in with
> `FlushPolicy::Buffered { max_bytes, max_records, max_idle }`. The rest of this
> document is the original design rationale and remains accurate about the
> *durability contract* and *trade-offs*; only the buffering *mechanism* differs.


The hotpath matrix showed that `Segment::append` (the `write()` syscall) is
41% of the per-send cost in the sequential scenario. This document explains
**why a BufWriter is the right tool for that cost line**, what trade-off
comes with it, and the implementation shape we should adopt.

## 1. Why we need BufWriter -- the problem

### 1.1 What the measurement shows

Sequential, 256 B records, no compression (one record per call, no caller-side
batching):

```
partition::append              5.43 us   100%
  flush_append_batch           3.83 us    70.5%   writer-side
    log::append_batch          3.12 us    57.5%
      segment::append          2.23 us    41%    <-- write() syscall
      sparse_index::track_append  302 ns   5.6%
      record::encode_with        114 ns   2.1%
  delta (mpsc + oneshot)       ~1.60 us   29.5%
```

`Segment::append` is **the single largest line item**. It is one full
user->kernel->user round-trip per record, and it fires 100 000 times in this
scenario.

### 1.2 What a write() syscall actually costs

At 256 B per record, the cost of `write()` is dominated by the syscall
*itself*, not by the bytes being moved:

- User -> kernel transition (syscall instruction, register save, ring change)
- File handle lookup in the process's fd table
- VFS dispatch + the filesystem driver's write path
- File position cursor update
- Return to userspace (register restore, ring change)

For a 256 B write going into the page cache, the byte copy is well under
100 ns. The remaining ~2 us is per-call fixed cost.

This means writing N small records costs `N * ~2 us` -- and writing one
combined block of (N * 256 B) costs the same ~2 us as writing a single 256 B
record. **Syscall overhead amortizes perfectly across the size of one call.**

### 1.3 The opportunity

If we issued one `write()` per 100 records instead of per record, the syscall
cost amortizes from 2.23 us per record to 22 ns per record -- a **100x
reduction on a line that is 41% of total per-send cost**. The expected effect
on `partition::append` mean: 5.43 us -> ~3.2 us, i.e. **~40% throughput
improvement** in the sequential scenario.

This is what BufWriter is for.

### 1.4 What about natural batching? Doesn't it already do this?

Partially. The writer task's natural batching coalesces commands that arrive
while it is busy with a previous flush -- so under concurrent load, multiple
records flow through one `Segment::append` call. The matrix's concurrent
scenario benefits from this.

But in the **sequential** scenario, each `send().await` round-trips fully
before the next one arrives. The writer task is idle when the next command
arrives. Natural batching has nothing to coalesce. One record per inbox
wake-up means one record per `Segment::append` call -- the worst case.

BufWriter is the optimization for the case natural batching cannot help:
**a single producer at maximum sequential throughput**.

## 2. What BufWriter mechanically does

`std::io::BufWriter<W>` wraps a writer with an internal `Vec<u8>` (default
size 8 KiB). On `write(&buf)`:

- If `buf` fits in the remaining BufWriter capacity -> memcpy into the
  internal Vec, return immediately. **Zero syscalls.**
- If `buf` would exceed capacity -> flush the existing buffer with one
  `write()`, then either append `buf` to the now-empty buffer or write it
  directly (depending on its size).

`flush()` issues one `write()` covering everything currently buffered.

Net effect for kafko: N small `Segment::append` calls -> **one** syscall per
buffer-full. The other (N-1) calls are pure userspace memcpy.

## 3. Where the trade-off lives -- the durability contract

The savings are real but they come with a contract change. This is the
central design decision; everything else is mechanical.

### 3.1 Current contract (kafko v0.2.0)

`Producer::send().await` resolves once the record bytes have returned from
`write_all()` -- i.e., they are in the **OS page cache**. This is the same
contract as Kafka `acks=1`:

- Process crash after this point: data is **not lost**. The page cache
  belongs to the kernel and survives process death.
- Power loss after this point: data **can be lost**. The page cache is in
  volatile DRAM and has not been fsync'd to disk yet.

### 3.2 Contract with naive BufWriter

If `Segment::append` writes into a BufWriter and returns immediately,
`Producer::send().await` would resolve once the bytes are in the **user-space
BufWriter** -- still in the kafko process's memory, not yet in the page
cache. This is **strictly weaker than Kafka `acks=0`**:

- Process crash: data **is lost**. Anything still in the BufWriter at crash
  time dies with the process.
- Power loss: same as before, can be lost.

This is a real, observable semantics change. We must not ship it silently.

### 3.3 The options

**Option A -- flush before reply (no BufWriter win)**

Buffer the write, but flush before sending the oneshot reply. Preserves the
contract perfectly. **Buys nothing in the sequential case** -- each send
still produces a syscall to flush its own bytes. Rejected.

**Option B -- deferred ack (recommended default)**

The writer task accumulates records in the BufWriter and **holds their
oneshot replies in a pending queue**. The flush is triggered by:

- buffer fills past a byte threshold (e.g., 64 KiB), OR
- a record count threshold (e.g., 1024), OR
- an idle timer fires (e.g., 200 us with no new commands)

When the flush completes, ONE `write()` covers all buffered records, and ALL
held oneshot replies are sent at once.

The current contract is preserved verbatim: bytes are in the page cache when
`send().await` resolves. The writer simply delays resolution to amortize the
syscall across multiple records. From the caller's perspective the behaviour
is: "your `send` waits a little longer than before; throughput is much
higher."

The cost: **latency tail for low-throughput producers**. A solitary record
arriving while the buffer is empty waits up to `idle_timeout` before its
reply is sent. This is tunable (e.g., 200 us default, configurable down to
zero to opt out per topic).

**Option C -- explicit acks levels (future work, not v0.2 scope)**

Add `LogConfig::ack_policy`:

- `AckPolicy::Buffered`  -- bytes in user-space buffer (process-crash unsafe)
- `AckPolicy::Written`   -- bytes in page cache (current behaviour)
- `AckPolicy::Synced`    -- bytes fsync'd to disk

Users opt into the speed/safety trade-off. Option C can layer on top of
Option B without redesigning anything.

### 3.4 Recommendation

**Ship Option B**. It captures most of the syscall-reduction win while
preserving the current durability contract verbatim, and it leaves room for
Option C later.

## 4. Implementation sketch -- Option B

### 4.1 Where the buffer lives

Inside `Segment`. It's the lowest layer that owns the `File` handle and the
only thing that issues `write()`. Moving the buffer higher (into `Log` or
`Partition`) would force every other Segment method to know about it.

```rust
pub struct Segment {
    base_offset: u64,
    path: PathBuf,
    file: BufWriter<File>,   // <-- new
    size: u64,
    cursor: Option<u64>,
    // ...
}
```

The capacity of the internal Vec stays tunable via `LogConfig`.

### 4.2 Where the pending replies live

In the partition writer task, alongside the BufWriter's lifetime. When the
writer task does an explicit `Log::append_batch`, the offsets it gets back
go into a `Vec<(oneshot::Sender<Result<u64>>, u64)>` *pending* queue. The
queue is drained by sending all the replies once the segment has been
flushed.

### 4.3 Flush triggers

A `FlushPolicy` enum on `LogConfig`:

```rust
pub enum FlushPolicy {
    /// Flush on every batch. Current v0.2.0 behaviour. Default for safety.
    EveryBatch,

    /// Buffer up to `max_bytes` of records or `max_records` count or
    /// `max_idle` time, whichever fires first. Replies are held until
    /// the flush completes.
    Buffered {
        max_bytes: usize,
        max_records: usize,
        max_idle: Duration,
    },
}
```

Default stays `EveryBatch`. Users opt in by setting `Buffered { ... }` on
the topic's `LogConfig`. Backwards compatible.

The writer task gains a `tokio::time::sleep_until` future tracking the idle
deadline. The select loop wakes on inbox commands, retention tick, OR idle
deadline.

### 4.4 What `Partition::sync()` does

Must flush the BufWriter first, then `sync_data()` on the underlying File.
No semantics change for callers.

### 4.5 What graceful shutdown does

The existing shutdown path (`partition_writer_loop` -> `log.sync()` on inbox
close) already flushes the active segment. It now needs to flush the
BufWriter too. The `Log::sync` -> `Segment::sync` chain becomes "BufWriter
flush + sync_data".

### 4.6 What crash recovery sees

Unchanged. Recovery scans the segment file with CRC verification on the
tail; whether the tail was produced by a per-record write or a coalesced
buffer flush makes no difference. Torn-tail truncate handles partial writes
either way.

## 5. Expected impact (re-measure after)

Re-running the hotpath matrix after BufWriter lands should show:

- `Segment::append` (now writing into a buffer): mean per call drops from
  2.23 us -> ~50 ns (a memcpy). Total drops proportionally.
- A new function `Segment::flush_buffer` (or whatever wraps the actual
  write): much lower call count, similar per-call cost to old
  `Segment::append`.
- `partition::append` mean: ~5.4 us -> ~3.2 us (40% headline win).
- Sequential scenario throughput: ~170k rec/s -> ~280k rec/s (estimate).
- Concurrent scenario: smaller improvement, because natural batching was
  already doing some of this work.
- Batch scenario: no improvement -- already one syscall per batch.

The recommended next step *after* BufWriter is decided by which line item is
then dominant. If the new dominant cost is mpsc round-trip (~1.6 us), the
LMAX-ring producer-to-writer queue becomes the next target. If it is
`record::encode_with` plus its compression child, the lz4_flex alloc fix
becomes the target.

## 6. Risks and unknowns

- **Latency tail for sparse traffic.** A topic with one send per second
  pays a full `max_idle` of latency on every send. The default 200 us is
  conservative -- well below the syscall savings -- but users with
  microsecond-sensitive paths should set `FlushPolicy::EveryBatch` or
  `max_idle = 0`.
- **Power-loss window grows slightly.** More unacked bytes can be in the
  page cache at any moment because more records are coalesced per write.
  `Partition::sync()` still gives a hard durability boundary; the window
  between syncs just contains more data.
- **Idle timer pressure.** Every topic with `Buffered` policy spawns one
  `sleep_until` future per inbox wake-up. Cheap, but worth verifying it
  doesn't oversubscribe the tokio runtime on a broker with thousands of
  idle topics.
- **`Segment::size` accounting.** Currently bumped after `write_all`
  returns. With BufWriter we need to bump it after the *append-into-buffer*
  returns (so rotation decisions are correct), but `would_overflow` and
  threshold checks must still treat buffered-but-not-flushed bytes as
  committed.

## 7. Out of scope for this change

- io_uring (Linux only; should be re-evaluated after BufWriter lands and
  syscalls are infrequent)
- `O_DIRECT` (incompatible with BufWriter by design)
- mmap-based segment writes (different optimization category)
- LMAX-ring producer-to-writer queue (separate cost line: the mpsc
  round-trip)
- `AckPolicy::Buffered` / `AckPolicy::Synced` (Option C; layers on later)

## 8. Files touched

- `crates/kafko/src/segment.rs` -- BufWriter wrapping the File
- `crates/kafko/src/log.rs` -- `FlushPolicy` enum, plumbing through to the
  writer task
- `crates/kafko/src/partition.rs` -- pending-replies queue, idle timer in
  the writer loop, flush-on-shutdown
- `crates/kafko-bench/src/main.rs` -- no change (re-runs the same matrix)
- `crates/kafko/tests/` -- new tests for the buffered path including
  process-crash-window expectations
