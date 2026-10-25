# Design note — Part 1: capacity by role; Part 2: ordering by type

Status: Parts 1–2 implemented. Part 3 (urgent reruns) later.

## Job lifecycle and what counts where

| Stage | Meaning | Counts as |
| --- | --- | --- |
| waiting | accepted, in a per-(role, type) FIFO queue | rerun demand (if rerun) |
| running | claimed by a worker until its function returns | one slot; rerun demand (if rerun) |
| done | function returned or raised | nothing |

There is no "granted but not started" stage: a worker claims a
job and runs it itself, in one step under the lock. Hence the
second half of C6 — a slot exists only while a function executes,
so nobody can hold one idle.

## Capacity rule

```
rerun_demand = min(rerun_max, waiting_rerun + running_rerun)
live_cap     = total - rerun_demand
```

A rerun may start iff `running_rerun < rerun_max` and a slot is
free (C1, C2); a live job iff `running_live < live_cap` and a slot
is free. Demand counts waiting *and* running reruns so the cap is
an invariant, not a race outcome: with one rerun running and one
waiting (`rerun_max=2`), counting only waiting work would reserve
one slot where two are needed, letting a live job take the
waiting rerun's slot.

- **C3/S3** — no rerun work: `live_cap = total`. One rerun with
  `rerun_max=2`, `total=4`: demand 1, so live may run 3 (reserving
  the full `rerun_max` would waste a slot).
- **C4/C5** — a rerun arriving at a full pool drops `live_cap`
  below `running_live`; no new live job starts, the next freed
  slot goes to the rerun, and live re-expands as rerun work
  drains. Capacity moves only at claim time; nothing is preempted.
- **C6** — if a waiting rerun cannot start, `running_rerun =
  rerun_max`, so demand equals `running_rerun` and live jobs can
  fill every remaining slot. If a live job is blocked by
  `live_cap`, demand exceeds `running_rerun`, so a rerun is
  waiting below its cap and is eligible. Some waiting job is
  therefore always eligible for a free slot, and workers re-check
  on every submit and finish. **C7**: workers block on a
  `Condition`; no polling.

Rejected: static partition (C3); always reserving `rerun_max`
(S3/C6); preemption (C5); paired semaphores, which cannot check
total, role cap, and demand atomically — the one lock can.

## Structure and ordering

`total` worker threads share all work (a per-role pool would
partition capacity, violating C3). Picking is two steps: role
first (capacity; rerun before live per C4, with `live_cap` — not
check order — enforcing the cap), then type within the role,
comparing only the two queue heads:

- quick beats full (O1), unless the full job has waited at least
  `starvation_after` seconds (K). Such a job is *promoted* into
  the quick tier, where it competes by submission order (O3).
- If only one queue is non-empty, its head runs.

O2 holds because, once promoted, a full job can only be preceded
by quick jobs submitted *before* it — a finite set, fixed at its
submission and only shrinking. No newly arriving quick job can
overtake it, so its wait is bounded by those earlier jobs plus
the running ones. Reversed, the same argument protects quick
jobs from promoted full jobs submitted after them: neither type
starves. Comparing heads is sound since the head full job is the
oldest of its role; if unpromoted, all behind it are too.

Aging is evaluated at pick time (`clock() - enqueued_at >= K`),
so no timer wakeup is needed: aging only reorders waiting jobs,
never makes one eligible — capacity does, on submit/finish,
which already notify. Time is stamped at submit, under the lock,
from the injected `clock` (default `time.monotonic`); waiting is
acceptance-to-claim. Every sample is validated finite, as is K
itself (an infinite K would silently disable O2). Tests inject a
fake clock. Cross-role, ordering remains capacity-driven (O1
allows this); aging is within-role only, as Part 1 already
guarantees each role its slots.

Complexity, for n waiting jobs: submit and pick are O(1) in n
(deque append; two heads; no heap, since queues are already in
submission order). One qualification: submit's `notify_all()`
can wake up to `total` workers, so its wall-clock cost is
O(total) — a small fixed pool size, independent of n. Memory is
O(active jobs): queues plus a set of in-flight job ids, each id
released when its job finishes.

## Concurrency hazards

- **Check-then-act** — eligibility, dequeue, and the running
  count change atomically under one lock; two workers cannot
  claim the last slot.
- **Lost wakeups** — submit, finish, and shutdown all
  `notify_all()` under that lock; workers re-check in a `while`
  loop, making spurious wakeups harmless.
- **User code under the lock** — functions run after unlocking;
  a slow or submitting job cannot stall the scheduler.
- **Failing jobs** — even `BaseException` (`SystemExit`,
  `KeyboardInterrupt` from job code) is caught by the worker,
  with the slot freed in `finally`; no exit path leaks capacity
  or kills a worker. The scheduler delivers no results or
  errors, so recording a failure is the job's own business.
- **Never-runnable work / shutdown** — with `rerun_max=0`,
  `submit()` rejects reruns immediately (caps are immutable)
  rather than accepting work that can never run. `shutdown()`
  always drains, then joins the workers; every accepted job can
  become eligible as work finishes, so the drain is total.
  Workers are non-daemon threads and callers must always shut
  down (directly or via `with`) — a forgotten shutdown blocks
  exit loudly instead of dropping work silently. Calling
  `shutdown()` after shutdown has already started is a no-op.

## Deliberately left out

Part 3 urgent reruns; result/error delivery (jobs record their
own outcomes); cancellation, timeouts, retries, persistence
(out of scope per the assignment).

## Multi-process / distributed

The single lock is what changes: counts and queues move to a
shared store, claiming needs compare-and-swap or a central
scheduler, and crashed workers need leases to reclaim slots.
The capacity and ordering *rules* are unchanged; only where
their inputs live and how a claim is made atomic.
