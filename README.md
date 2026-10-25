# Mini eval scheduler

An in-memory job scheduler for an evaluation platform: a fixed
pool of worker threads runs jobs, with capacity split by role
(`live` vs `rerun`), ordering by type (`quick` before `full`,
with anti-starvation aging), and an `urgent` flag for reruns.
See [DESIGN.md](DESIGN.md) for the design note.

## Requirements

Python 3.11 or newer. No third-party dependencies.

## Run the tests

```
python3 -m unittest -v
```

That command discovers and runs `test_scheduler.py`.

## Use it

```python
from scheduler import JobType, Role, Scheduler

with Scheduler(total=4, rerun_max=1) as scheduler:
    scheduler.submit("job-1", Role.LIVE, JobType.QUICK, lambda: print("hi"))
    scheduler.submit(
        "job-2",
        Role.RERUN,
        JobType.FULL,
        lambda: print("urgent rerun"),
        urgent=True,
    )
# Leaving the `with` block calls shutdown(): no new jobs are
# accepted, queued work drains, then the workers exit.
```

Notes for callers:

- `total` is the maximum number of jobs running at once;
  `rerun_max` caps how many of those may be reruns. Leaving out
  the `with` (or an explicit `shutdown()`) blocks process exit,
  by design — workers are non-daemon threads.
- `submit()` never blocks and returns nothing. The scheduler
  does not deliver results or errors: a job records its own
  outcome (e.g. by setting an `Event` or writing to a list).
- Job ids must be unique among active (waiting or running)
  jobs, and may be reused once a job finishes.
- `starvation_after` is K from the assignment (default 30s):
  a `full` job waiting that long stops being overtaken by new
  `quick` jobs in the same urgency tier. `clock` is injectable for
  deterministic tests (default `time.monotonic`).
- `scheduler.stats()` returns a waiting/running count snapshot.

## Files

- `scheduler.py` — the scheduler (single module, stdlib only).
- `test_scheduler.py` — the test suite (`unittest`).
- `DESIGN.md` — design note: capacity rule, ordering,
  concurrency hazards, Part 3, and what was left out.
