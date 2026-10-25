# Design note — Parts 1–3

## Job lifecycle and what counts where

| Stage | Meaning | Counts as |
| --- | --- | --- |
| waiting | accepted, in a per-(role, urgent, type) FIFO queue | rerun demand (if rerun) |
| running | claimed by a worker until its function returns | one slot; rerun demand (if rerun) |
| done | function returned or raised | nothing |

A worker claims and accounts a job under the lock, then runs it
itself. There is no separate granted stage and idle workers hold
no slots.

## Capacity rule

```
rerun_demand = min(rerun_max, waiting_rerun + running_rerun)
live_cap     = total - rerun_demand
```

A rerun starts only below `rerun_max` and when a total slot is
free; a live job starts only below `live_cap`. Waiting and running
reruns both count as demand, so a started rerun does not
accidentally give its reserved capacity back to live.

With no rerun work, `live_cap = total` (C3). Rerun demand lowers
the cap without interrupting running live jobs; freed slots go to
reruns until demand is met, then return to live as reruns finish
(C4–C5). Reserving only actual demand avoids idle capacity (C6).
Workers block on a `Condition`, so there is no polling (C7).

I rejected static partitions, always reserving `rerun_max`, and
preemption because they violate C3, C6, and C5 respectively.

## Structure and ordering

`total` shared workers serve every role; separate pools would
partition capacity and violate C3. There is one FIFO deque per
`(role, urgency, type)`. Selection applies role capacity first,
then urgency for reruns, then type.

Within a tier, quick beats full (O1). Once the oldest full job has
waited K seconds it joins the quick tier and competes by submission
sequence (O2–O3). New quick jobs cannot pass it; only quick jobs
submitted earlier can remain ahead. Comparing queue heads is enough
because each deque is FIFO.

The injected clock is sampled at submission and selection. Aging
needs no timer: it changes order only when capacity becomes free,
which already wakes workers. Submission and selection are O(1) in
the number of waiting jobs; memory is O(waiting + running jobs).

## Urgent reruns (Part 3)

Urgency is an ordering rule within reruns, not extra capacity.
Urgent and normal jobs have identical capacity accounting and no
job is preempted.

Urgent reruns are selected first, but after three urgent claims
made while normal work waits, the next rerun claim must be normal.
Thus normal work receives at least one in every four rerun claims
under sustained contention.

Type ordering and aging apply within each urgency tier. Across
tiers, Part 3 deliberately relaxes O1/O2: an urgent full can beat a
normal quick, and an aged normal full can be passed by at most three
urgent claims. This bounded exception makes urgency meaningful
without allowing normal reruns to starve.

I rejected strict urgent priority because it can starve normal
reruns, and separate urgent capacity because it can waste slots.

## Concurrency hazards

- **Racing claims:** eligibility, dequeue, and running counters
  change atomically under one lock.
- **Lost wakeups:** submit, completion, and shutdown notify under
  that lock; workers always re-check in a loop.
- **Slow user code:** jobs execute outside the lock.
- **Failures:** `finally` releases capacity even when job code
  raises; errors are intentionally not delivered by the scheduler.
- **Shutdown:** submissions stop, queued work drains, and workers
  join. Reruns are rejected when `rerun_max=0` because they could
  never run.

## Deliberately left out

Result/error delivery and any logging of job failures (jobs
record their own outcomes; the scheduler never sees them);
cancellation, timeouts, retries, persistence (out of scope per
the assignment).

## Multi-process / distributed

The single lock is what changes: counts and queues move to a
shared store, claiming needs compare-and-swap or a central
scheduler, and crashed workers need leases to reclaim slots.
The capacity and ordering *rules* are unchanged; only where
their inputs live and how a claim is made atomic.
