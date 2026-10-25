"""In-memory job scheduler -- capacity by role; ordering by type and urgency.

A fixed pool of ``total`` worker threads runs jobs. Each running job
holds one slot. Role decides how much capacity a job may use:

    rerun_demand = min(rerun_max, waiting_rerun + running_rerun)
    live_cap     = total - rerun_demand

A waiting rerun may start when running_rerun < rerun_max and a slot
is free. A waiting live job may start when running_live < live_cap
and a slot is free. See DESIGN.md for why both waiting *and* running
reruns count towards demand.

Within a role, quick jobs run before full jobs, except that a full
job waiting at least ``starvation_after`` seconds (the assignment's
K) is promoted into the quick tier, where it is ordered by
submission. Time comes from the injectable ``clock`` callable, so
tests control it deterministically.

Part 3: reruns may be marked urgent. Urgency is an ordering key
within reruns only -- it changes neither capacity nor the
live/rerun split. Urgent reruns are picked before normal ones,
but at most ``_MAX_URGENT_STREAK`` in a row while a normal rerun
is waiting, so normal reruns always make progress.
"""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Role(Enum):
    LIVE = "live"
    RERUN = "rerun"


class JobType(Enum):
    QUICK = "quick"
    FULL = "full"


# Part 3: max consecutive urgent rerun claims while a normal rerun waits.
_MAX_URGENT_STREAK = 3


@dataclass
class _Job:
    job_id: str
    role: Role
    job_type: JobType
    urgent: bool  # reruns only; an ordering key, not extra capacity
    fn: Callable[[], Any]
    enqueued_at: float  # per the injectable clock, stamped at submit
    seq: int  # submission order across all jobs, under the lock


class Scheduler:
    """Runs submitted jobs on a shared pool (capacity, type ordering, urgency).

    ``starvation_after`` is K from O2, in clock seconds. ``clock`` is
    the only source of time; it must return finite numbers, be
    non-decreasing and cheap, since it is called under the lock at
    submit and at pick time.
    """

    def __init__(
        self,
        total: int,
        rerun_max: int,
        starvation_after: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not isinstance(total, int) or isinstance(total, bool) or total < 1:
            raise ValueError("total must be an int >= 1")
        if (
            not isinstance(rerun_max, int)
            or isinstance(rerun_max, bool)
            or not 0 <= rerun_max <= total
        ):
            raise ValueError("rerun_max must be an int with 0 <= rerun_max <= total")
        if (
            not isinstance(starvation_after, (int, float))
            or isinstance(starvation_after, bool)
            or not math.isfinite(starvation_after)  # inf would break O2
            or starvation_after < 0
        ):
            raise ValueError("starvation_after must be a finite number >= 0")
        if not callable(clock):
            raise ValueError("clock must be callable")

        self._total = total
        self._rerun_max = rerun_max
        self._starvation_after = float(starvation_after)
        self._clock = clock

        # All mutable state below is guarded by this one condition's lock.
        # One FIFO queue per (role, urgent, type): submit and pick
        # stay O(1) no matter how many jobs are waiting (O4). The
        # live/urgent queues stay empty; submit rejects that combo.
        self._cond = threading.Condition()
        self._waiting: dict[tuple[Role, bool, JobType], deque[_Job]] = {
            (role, urgent, job_type): deque()
            for role in Role
            for urgent in (False, True)
            for job_type in JobType
        }
        self._running_live = 0
        self._running_rerun = 0
        self._urgent_streak = 0  # urgent rerun claims since last normal one
        self._next_seq = 0
        # Ids of waiting/running jobs only; an id is released when
        # its job finishes, so memory stays O(active jobs).
        self._active_ids: set[str] = set()
        self._shutdown = False

        # One worker per slot: an idle worker therefore never holds a
        # slot (C6), and a free slot always has a worker to fill it.
        # Workers are non-daemon threads: a forgotten shutdown()
        # blocks process exit instead of silently abandoning work.
        # Callers must always call shutdown() (or use ``with``).
        self._workers = [
            threading.Thread(target=self._worker_loop, name=f"worker-{i}", daemon=False)
            for i in range(total)
        ]
        for worker in self._workers:
            worker.start()

    # -- public API -----------------------------------------------------

    def submit(
        self,
        job_id: str,
        role: Role,
        job_type: JobType,
        fn: Callable[[], Any],
        urgent: bool = False,
    ) -> None:
        """Queue a job without blocking; it starts when capacity allows.

        Job ids must be unique among active (waiting or running)
        jobs; an id may be reused once its previous job finished.
        The scheduler does not deliver outcomes: return values are
        ignored, so jobs record their own results and errors.
        ``urgent`` (reruns only) orders a rerun ahead of normal
        reruns; it grants no extra capacity.
        """
        if not isinstance(job_id, str) or not job_id:
            raise ValueError("job_id must be a non-empty string")
        if not isinstance(role, Role):
            raise ValueError("role must be Role.LIVE or Role.RERUN")
        if not isinstance(job_type, JobType):
            raise ValueError("job_type must be JobType.QUICK or JobType.FULL")
        if not callable(fn):
            raise ValueError("fn must be callable")
        if not isinstance(urgent, bool):
            raise ValueError("urgent must be a bool")
        if urgent and role is Role.LIVE:
            raise ValueError("only reruns can be urgent")
        if role is Role.RERUN and self._rerun_max == 0:
            # Caps never change after construction, so this job could
            # never start. Refuse it now instead of accepting work
            # that can never run.
            raise ValueError("rerun jobs cannot run when rerun_max is 0")

        with self._cond:
            if self._shutdown:
                raise RuntimeError("scheduler is shut down")
            if job_id in self._active_ids:
                raise ValueError(f"duplicate job_id {job_id!r}")
            # Stamp time before consuming the id, so a clock failure
            # leaves no half-registered job behind. Stamping inside
            # the lock keeps time and sequence in queue order.
            enqueued_at = self._clock_now_locked()
            self._active_ids.add(job_id)
            job = _Job(
                job_id=job_id,
                role=role,
                job_type=job_type,
                urgent=urgent,
                fn=fn,
                enqueued_at=enqueued_at,
                seq=self._next_seq,
            )
            self._next_seq += 1
            self._waiting[(role, urgent, job_type)].append(job)
            self._cond.notify_all()

    def stats(self) -> dict[str, int]:
        """Snapshot of waiting/running counts, taken under the lock."""
        with self._cond:
            return {
                "waiting_live": self._waiting_job_count_locked(Role.LIVE),
                "waiting_rerun": self._waiting_job_count_locked(Role.RERUN),
                "running_live": self._running_live,
                "running_rerun": self._running_rerun,
            }

    def shutdown(self) -> None:
        """Stop accepting jobs, drain queued work, then exit workers.

        Always waits for the drain to finish. Calling it after shutdown
        has already started is a no-op. Callers must always call this
        (workers are non-daemon threads), directly or via ``with``.
        """
        with self._cond:
            if self._shutdown:
                return
            self._shutdown = True
            self._cond.notify_all()
        for worker in self._workers:
            worker.join()

    def __enter__(self) -> Scheduler:
        return self

    def __exit__(self, *exc: object) -> None:
        self.shutdown()

    # -- internals (called with the lock held unless noted) -------------

    def _clock_now_locked(self) -> float:
        """One clock sample, enforcing the finite-value contract."""
        now = float(self._clock())
        if not math.isfinite(now):
            raise ValueError("clock must return finite numbers")
        return now

    def _waiting_count_for_tier_locked(self, role: Role, urgent: bool) -> int:
        return sum(len(self._waiting[(role, urgent, t)]) for t in JobType)

    def _waiting_job_count_locked(self, role: Role) -> int:
        return self._waiting_count_for_tier_locked(
            role, False
        ) + self._waiting_count_for_tier_locked(
            role, True
        )

    def _maximum_live_jobs_locked(self) -> int:
        rerun_demand = min(
            self._rerun_max,
            self._waiting_job_count_locked(Role.RERUN) + self._running_rerun,
        )
        return self._total - rerun_demand

    def _dequeue_next_job_locked(self, role: Role) -> _Job:
        """Dequeue the next job of one role (call only if it has waiting work).

        For reruns the urgency tier is chosen first: urgent before
        normal, but after ``_MAX_URGENT_STREAK`` consecutive urgent
        claims made while a normal rerun was waiting, the next
        claim must be normal and the streak resets. Within the
        chosen tier, type ordering (see ``_dequeue_job_by_type_locked``)
        applies unchanged. Live jobs have a single tier.
        """
        if role is Role.LIVE:
            return self._dequeue_job_by_type_locked(role, urgent=False)

        urgent_n = self._waiting_count_for_tier_locked(Role.RERUN, True)
        normal_n = self._waiting_count_for_tier_locked(Role.RERUN, False)
        if urgent_n and normal_n and self._urgent_streak >= _MAX_URGENT_STREAK:
            urgent = False  # burst exhausted: normal rerun's turn
        elif urgent_n:
            urgent = True
        else:
            urgent = False
        job = self._dequeue_job_by_type_locked(Role.RERUN, urgent)
        # Only urgent claims that actually delayed a waiting normal
        # rerun count towards the burst; any other claim resets it.
        self._urgent_streak = self._urgent_streak + 1 if urgent and normal_n else 0
        return job

    def _dequeue_job_by_type_locked(self, role: Role, urgent: bool) -> _Job:
        """Dequeue the next job of one urgency tier by type ordering.

        Quick beats full (O1), except a full job that has waited at
        least K is promoted into the quick tier: promoted full and
        quick compete by submission order (O3). Only the two queue
        heads are compared -- the head full job is the oldest full
        job of this tier, so if it is not promoted none behind it
        is either. Aging is evaluated here, at pick time, from the
        injected clock; no timer wakeup is needed because aging only
        reorders waiting jobs, it never makes a job eligible.
        """
        quick_q = self._waiting[(role, urgent, JobType.QUICK)]
        full_q = self._waiting[(role, urgent, JobType.FULL)]
        if quick_q and full_q:
            full = full_q[0]
            promoted = (
                self._clock_now_locked() - full.enqueued_at
                >= self._starvation_after
            )
            if promoted and full.seq < quick_q[0].seq:
                return full_q.popleft()
            return quick_q.popleft()
        if quick_q:
            return quick_q.popleft()
        return full_q.popleft()

    def _claim_eligible_job_locked(self) -> _Job | None:
        """Claim the next eligible job and account it as running, or None.

        Role (capacity) is decided first, then urgency (reruns),
        then type. Reruns are checked first so a freed slot goes
        to a waiting rerun before a waiting live job (C4). Urgency
        never affects eligibility here -- only which rerun is
        claimed. Claiming is atomic with the capacity check
        because both happen under the lock.
        """
        if self._running_live + self._running_rerun >= self._total:
            return None
        if (
            self._waiting_job_count_locked(Role.RERUN)
            and self._running_rerun < self._rerun_max
        ):
            job = self._dequeue_next_job_locked(Role.RERUN)
            self._running_rerun += 1
            return job
        if (
            self._waiting_job_count_locked(Role.LIVE)
            and self._running_live < self._maximum_live_jobs_locked()
        ):
            job = self._dequeue_next_job_locked(Role.LIVE)
            self._running_live += 1
            return job
        return None

    def _worker_loop(self) -> None:
        while True:
            with self._cond:
                while True:
                    job = self._claim_eligible_job_locked()
                    if job is not None:
                        break
                    # Nothing eligible. Exit only once shutdown leaves
                    # no work that could still become eligible (a still-
                    # running job finishing can make waiting work eligible).
                    running = self._running_live + self._running_rerun
                    if self._shutdown and running == 0:
                        return
                    self._cond.wait()  # no busy-waiting (C7)

            # Run outside the lock: jobs may be long, and may submit work.
            # Even BaseException (SystemExit, KeyboardInterrupt from
            # job code) must not leak the slot or kill the worker, so
            # it is caught here; recording failures is the job's own
            # business. The finally block frees the slot on every
            # exit path.
            try:
                job.fn()
            except BaseException:
                pass
            finally:
                with self._cond:
                    if job.role is Role.LIVE:
                        self._running_live -= 1
                    else:
                        self._running_rerun -= 1
                    self._active_ids.discard(job.job_id)
                    self._cond.notify_all()
