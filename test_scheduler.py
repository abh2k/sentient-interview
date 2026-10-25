"""Tests for Part 1 (capacity) and Part 2 (ordering). Run: python3 -m unittest -v

Determinism: gated jobs block on an Event until the test releases
them, so "which jobs are running" is observed exactly, not raced;
Part 2 aging uses a FakeClock advanced by hand. The only real-time
waits are bounded negative waits ("this must NOT start").
"""

import threading
import unittest

from scheduler import JobType, Role, Scheduler

START_TIMEOUT = 2.0  # generous: a correct scheduler starts jobs at once
NOT_STARTED_TIMEOUT = 0.25  # bounded wait to prove a job stays queued


class GatedJob:
    """A job that signals it started, then blocks until released."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    def __call__(self) -> None:
        self.started.set()
        self.release.wait()
        self.finished.set()


class FakeClock:
    """Manually advanced clock injected into the scheduler (Part 2)."""

    def __init__(self) -> None:
        self._now = 0.0
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += seconds


class SchedulerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._gates: list[GatedJob] = []
        self._schedulers: list[Scheduler] = []

    def tearDown(self) -> None:
        for gate in self._gates:
            gate.release.set()
        for scheduler in self._schedulers:
            scheduler.shutdown()

    def make_scheduler(self, total: int, rerun_max: int, **kwargs) -> Scheduler:
        scheduler = Scheduler(total=total, rerun_max=rerun_max, **kwargs)
        self._schedulers.append(scheduler)
        return scheduler

    def gated(
        self,
        scheduler: Scheduler,
        job_id: str,
        role: Role,
        job_type: JobType = JobType.QUICK,
    ) -> GatedJob:
        job = GatedJob(job_id)
        self._gates.append(job)
        scheduler.submit(job_id, role, job_type, job)
        return job

    def assertFired(self, event: threading.Event) -> None:
        self.assertTrue(event.wait(START_TIMEOUT), "event should have fired")

    def assertStarted(self, job: GatedJob) -> None:
        self.assertTrue(
            job.started.wait(START_TIMEOUT), f"{job.name} should have started"
        )

    def assertNotStarted(self, job: GatedJob) -> None:
        self.assertFalse(
            job.started.wait(NOT_STARTED_TIMEOUT),
            f"{job.name} should still be waiting",
        )


class StarterScenarios(SchedulerTestCase):
    def test_s1_no_rerun_work_live_uses_all_slots(self) -> None:
        scheduler = self.make_scheduler(total=4, rerun_max=1)
        jobs = [self.gated(scheduler, f"L{i}", Role.LIVE) for i in range(6)]

        for job in jobs[:4]:
            self.assertStarted(job)
        for job in jobs[4:]:
            self.assertNotStarted(job)
        self.assertEqual(
            scheduler.stats(),
            {"waiting_live": 2, "waiting_rerun": 0,
             "running_live": 4, "running_rerun": 0},
        )

        # Freeing one slot starts the next waiting job (C6: no waste).
        jobs[0].release.set()
        self.assertStarted(jobs[4])
        self.assertNotStarted(jobs[5])

    def test_s2_next_freed_slot_goes_to_rerun(self) -> None:
        scheduler = self.make_scheduler(total=4, rerun_max=1)
        running = [self.gated(scheduler, f"L{i}", Role.LIVE) for i in range(4)]
        for job in running:
            self.assertStarted(job)

        waiting_live = self.gated(scheduler, "L4", Role.LIVE)
        rerun = self.gated(scheduler, "R0", Role.RERUN)
        self.assertNotStarted(waiting_live)
        self.assertNotStarted(rerun)

        # Running jobs are never interrupted (C5); the rerun wins the
        # first freed slot even though L4 waited longer (C4).
        running[0].release.set()
        self.assertStarted(rerun)
        self.assertNotStarted(waiting_live)

    def test_s3_one_running_rerun_reserves_only_one_slot(self) -> None:
        # rerun_max=2 but only one rerun exists: live may use the
        # other 3 slots. Reserving both rerun slots would waste one.
        scheduler = self.make_scheduler(total=4, rerun_max=2)
        rerun = self.gated(scheduler, "R0", Role.RERUN)
        self.assertStarted(rerun)

        jobs = [self.gated(scheduler, f"L{i}", Role.LIVE) for i in range(4)]
        for job in jobs[:3]:
            self.assertStarted(job)
        self.assertNotStarted(jobs[3])
        self.assertEqual(scheduler.stats()["running_live"], 3)


class CapacityRules(SchedulerTestCase):
    def test_c2_rerun_cap_holds_even_with_free_slots(self) -> None:
        scheduler = self.make_scheduler(total=4, rerun_max=1)
        jobs = [self.gated(scheduler, f"R{i}", Role.RERUN) for i in range(3)]

        self.assertStarted(jobs[0])
        self.assertNotStarted(jobs[1])
        self.assertNotStarted(jobs[2])
        self.assertEqual(scheduler.stats()["running_rerun"], 1)

    def test_live_gets_capacity_back_once_reruns_drain(self) -> None:
        scheduler = self.make_scheduler(total=2, rerun_max=1)
        rerun = self.gated(scheduler, "R0", Role.RERUN)
        live1 = self.gated(scheduler, "L0", Role.LIVE)
        live2 = self.gated(scheduler, "L1", Role.LIVE)
        self.assertStarted(rerun)
        self.assertStarted(live1)
        self.assertNotStarted(live2)

        # C4 second half + C6: the finish notification alone must let
        # the waiting live job claim the freed slot.
        rerun.release.set()
        self.assertStarted(live2)

    def test_live_cap_tracks_total_rerun_work(self) -> None:
        # total=5, rerun_max=2, two rerun jobs exist (one running, one
        # waiting): live_cap = 5 - 2 = 3, so exactly 3 live jobs run.
        scheduler = self.make_scheduler(total=5, rerun_max=2)
        rerun1 = self.gated(scheduler, "R0", Role.RERUN)
        live1 = self.gated(scheduler, "L0", Role.LIVE)
        live2 = self.gated(scheduler, "L1", Role.LIVE)
        self.assertStarted(rerun1)
        self.assertStarted(live1)
        self.assertStarted(live2)

        rerun2 = self.gated(scheduler, "R1", Role.RERUN)
        live3 = self.gated(scheduler, "L2", Role.LIVE)
        live4 = self.gated(scheduler, "L3", Role.LIVE)
        self.assertStarted(rerun2)
        self.assertStarted(live3)
        self.assertNotStarted(live4)
        self.assertEqual(
            scheduler.stats(),
            {"waiting_live": 1, "waiting_rerun": 0,
             "running_live": 3, "running_rerun": 2},
        )

    def test_rerun_rejected_when_rerun_max_is_zero(self) -> None:
        # A rerun submitted here could never start, so submit refuses
        # it rather than accepting work that can never run. Live
        # work is unaffected, and shutdown has nothing abandoned.
        scheduler = self.make_scheduler(total=2, rerun_max=0)
        with self.assertRaises(ValueError):
            scheduler.submit("R0", Role.RERUN, JobType.QUICK, lambda: None)
        with self.assertRaises(ValueError):
            scheduler.submit("R1", Role.RERUN, JobType.FULL, lambda: None)

        # A rejected submit queued nothing and consumed no job id:
        # "R0" is accepted as a live job and runs.
        recorded: list[int] = []
        scheduler.submit("R0", Role.LIVE, JobType.QUICK, lambda: recorded.append(-1))
        for i in range(3):
            scheduler.submit(
                f"L{i}", Role.LIVE, JobType.QUICK, lambda i=i: recorded.append(i)
            )
        scheduler.shutdown()
        self.assertEqual(sorted(recorded), [-1, 0, 1, 2])
        self.assertEqual(
            scheduler.stats(),
            {"waiting_live": 0, "waiting_rerun": 0,
             "running_live": 0, "running_rerun": 0},
        )


class Behaviour(SchedulerTestCase):
    def test_fifo_within_a_role(self) -> None:
        scheduler = self.make_scheduler(total=1, rerun_max=1)
        order: list[int] = []
        for i in range(3):
            scheduler.submit(
                f"L{i}", Role.LIVE, JobType.QUICK, lambda i=i: order.append(i)
            )
        scheduler.shutdown()  # drains the queue
        self.assertEqual(order, [0, 1, 2])

    def test_failing_job_frees_its_slot(self) -> None:
        scheduler = self.make_scheduler(total=1, rerun_max=1)

        def boom() -> None:
            raise RuntimeError("boom")

        good_ran = threading.Event()
        scheduler.submit("bad", Role.LIVE, JobType.QUICK, boom)
        scheduler.submit("good", Role.LIVE, JobType.QUICK, good_ran.set)
        self.assertFired(good_ran)
        scheduler.shutdown()
        self.assertEqual(scheduler.stats()["running_live"], 0)

    def test_base_exception_frees_slot_and_worker_survives(self) -> None:
        # KeyboardInterrupt/SystemExit are BaseException, not
        # Exception. With one worker, a leaked slot or dead worker
        # would stop every following job from ever running.
        scheduler = self.make_scheduler(total=1, rerun_max=1)

        def bail() -> None:
            raise KeyboardInterrupt

        good_ran = threading.Event()
        scheduler.submit("bad", Role.LIVE, JobType.QUICK, bail)
        scheduler.submit("good", Role.LIVE, JobType.QUICK, good_ran.set)
        self.assertFired(good_ran)
        scheduler.shutdown()  # must return: worker is still alive
        self.assertEqual(
            scheduler.stats(),
            {"waiting_live": 0, "waiting_rerun": 0,
             "running_live": 0, "running_rerun": 0},
        )

    def test_concurrent_submissions_never_exceed_caps(self) -> None:
        # The first four jobs (3 live + 1 rerun) rendezvous on a
        # barrier that only trips if all four run at the same
        # instant. A scheduler that serialised jobs would break the
        # barrier and fail here, so the peaks are exact values, not
        # merely upper bounds. FIFO queues guarantee these four are
        # claimed first even though other submissions race them.
        total, rerun_max, n_other = 4, 1, 56
        scheduler = self.make_scheduler(total=total, rerun_max=rerun_max)
        barrier = threading.Barrier(total)
        lock = threading.Lock()
        current = {"live": 0, "rerun": 0}
        peak = {"live": 0, "rerun": 0, "all": 0}
        completed = 0

        def work(role: Role, rendezvous: bool = False) -> None:
            nonlocal completed
            key = "live" if role is Role.LIVE else "rerun"
            with lock:
                current[key] += 1
                peak[key] = max(peak[key], current[key])
                peak["all"] = max(peak["all"], current["live"] + current["rerun"])
            try:
                if rendezvous:
                    barrier.wait(timeout=5)
            finally:
                with lock:
                    current[key] -= 1
                    completed += 1

        for i, role in enumerate([Role.LIVE, Role.LIVE, Role.LIVE, Role.RERUN]):
            scheduler.submit(f"B{i}", role, JobType.QUICK, lambda r=role: work(r, True))

        def submit_batch(start: int) -> None:
            for i in range(start, n_other, 6):
                role = Role.RERUN if i % 3 == 0 else Role.LIVE
                scheduler.submit(f"J{i}", role, JobType.QUICK, lambda r=role: work(r))

        threads = [threading.Thread(target=submit_batch, args=(s,)) for s in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        scheduler.shutdown()  # drains everything still queued

        self.assertEqual(completed, total + n_other)
        self.assertEqual(peak["all"], total)        # reached, and never above (C1)
        self.assertEqual(peak["rerun"], rerun_max)  # reached, and never above (C2)
        self.assertEqual(
            scheduler.stats(),
            {"waiting_live": 0, "waiting_rerun": 0,
             "running_live": 0, "running_rerun": 0},
        )

    def test_validation_and_shutdown(self) -> None:
        with self.assertRaises(ValueError):
            Scheduler(total=0, rerun_max=0)
        with self.assertRaises(ValueError):
            Scheduler(total=2, rerun_max=3)
        with self.assertRaises(ValueError):
            Scheduler(total=2, rerun_max=-1)

        scheduler = self.make_scheduler(total=1, rerun_max=1)
        gate = self.gated(scheduler, "a", Role.LIVE)
        self.assertStarted(gate)
        with self.assertRaises(ValueError):
            scheduler.submit("a", Role.LIVE, JobType.QUICK, lambda: None)  # active id
        with self.assertRaises(ValueError):
            scheduler.submit("b", "live", JobType.QUICK, lambda: None)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            scheduler.submit("b2", Role.LIVE, "quick", lambda: None)  # type: ignore[arg-type]
        gate.release.set()
        scheduler.shutdown()
        with self.assertRaises(RuntimeError):
            scheduler.submit("c", Role.LIVE, JobType.QUICK, lambda: None)

    def test_job_id_reusable_after_completion(self) -> None:
        # Ids are only unique among active jobs, so scheduler memory
        # does not grow with every job ever submitted. The sentinel
        # job matters: on a one-worker scheduler it can only run
        # after the first job fully finished and freed its id.
        scheduler = self.make_scheduler(total=1, rerun_max=1)
        ran: list[str] = []
        sentinel_ran = threading.Event()
        scheduler.submit("x", Role.LIVE, JobType.QUICK, lambda: ran.append("first"))

        def sentinel() -> None:
            ran.append("sentinel")
            sentinel_ran.set()

        scheduler.submit("s", Role.LIVE, JobType.QUICK, sentinel)
        self.assertFired(sentinel_ran)
        scheduler.submit("x", Role.LIVE, JobType.QUICK, lambda: ran.append("second"))
        scheduler.shutdown()
        self.assertEqual(ran, ["first", "sentinel", "second"])

    def test_repeated_shutdown_is_a_no_op(self) -> None:
        scheduler = self.make_scheduler(total=1, rerun_max=1)
        ran = threading.Event()
        scheduler.submit("job", Role.LIVE, JobType.QUICK, ran.set)
        self.assertFired(ran)
        scheduler.shutdown()
        scheduler.shutdown()


class Ordering(SchedulerTestCase):
    """Part 2: one slot held by a blocker, so waiting order is exact."""

    def blocked(self, k: float = 10.0):
        clock = FakeClock()
        scheduler = self.make_scheduler(
            total=1, rerun_max=1, starvation_after=k, clock=clock
        )
        blocker = self.gated(scheduler, "BLOCK", Role.LIVE)
        self.assertStarted(blocker)
        return scheduler, clock, blocker

    def test_o1_quick_runs_before_full_in_same_role(self) -> None:
        scheduler, _, blocker = self.blocked()
        full = self.gated(scheduler, "F", Role.LIVE, JobType.FULL)
        quick = self.gated(scheduler, "Q", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assertStarted(quick)
        self.assertNotStarted(full)
        quick.release.set()
        self.assertStarted(full)

    def test_o2_full_aged_exactly_k_blocks_new_quick(self) -> None:
        scheduler, clock, blocker = self.blocked(k=10.0)
        full = self.gated(scheduler, "F", Role.LIVE, JobType.FULL)
        clock.advance(10.0)
        quick = self.gated(scheduler, "Q", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assertStarted(full)
        self.assertNotStarted(quick)

    def test_young_full_does_not_block_quick(self) -> None:
        scheduler, clock, blocker = self.blocked(k=10.0)
        full = self.gated(scheduler, "F", Role.LIVE, JobType.FULL)
        clock.advance(9.5)
        quick = self.gated(scheduler, "Q", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assertStarted(quick)
        self.assertNotStarted(full)

    def test_promoted_full_merges_by_submission_order(self) -> None:
        # Q1 submitted before F stays ahead of it; Q2 submitted after
        # F does not. Promotion puts F into the quick tier, it does
        # not jump it ahead of every quick job.
        scheduler, clock, blocker = self.blocked(k=10.0)
        q1 = self.gated(scheduler, "Q1", Role.LIVE, JobType.QUICK)
        full = self.gated(scheduler, "F", Role.LIVE, JobType.FULL)
        clock.advance(20.0)
        q2 = self.gated(scheduler, "Q2", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assertStarted(q1)
        self.assertNotStarted(full)
        q1.release.set()
        self.assertStarted(full)
        self.assertNotStarted(q2)

    def test_full_not_starved_by_a_stream_of_quicks(self) -> None:
        scheduler, clock, blocker = self.blocked(k=10.0)
        full = self.gated(scheduler, "F", Role.LIVE, JobType.FULL)
        quicks = [
            self.gated(scheduler, f"Q{i}", Role.LIVE, JobType.QUICK)
            for i in range(4)
        ]
        clock.advance(100.0)

        blocker.release.set()
        self.assertStarted(full)
        for quick in quicks:
            self.assertNotStarted(quick)

    def test_o3_fifo_within_each_type(self) -> None:
        scheduler, _, blocker = self.blocked(k=1000.0)
        order: list[str] = []

        def record(name: str) -> None:
            order.append(name)

        scheduler.submit("F1", Role.LIVE, JobType.FULL, lambda: record("F1"))
        scheduler.submit("Q1", Role.LIVE, JobType.QUICK, lambda: record("Q1"))
        scheduler.submit("F2", Role.LIVE, JobType.FULL, lambda: record("F2"))
        scheduler.submit("Q2", Role.LIVE, JobType.QUICK, lambda: record("Q2"))
        blocker.release.set()
        scheduler.shutdown()
        self.assertEqual(order, ["Q1", "Q2", "F1", "F2"])

    def test_ordering_applies_within_rerun_role_too(self) -> None:
        scheduler, _, blocker = self.blocked()
        rfull = self.gated(scheduler, "RF", Role.RERUN, JobType.FULL)
        rquick = self.gated(scheduler, "RQ", Role.RERUN, JobType.QUICK)

        blocker.release.set()
        self.assertStarted(rquick)
        self.assertNotStarted(rfull)

    def test_aging_does_not_override_role_capacity(self) -> None:
        # An aged live full job must not take the rerun's slot:
        # Part 1 (role) is decided before Part 2 (type).
        clock = FakeClock()
        scheduler = self.make_scheduler(
            total=2, rerun_max=1, starvation_after=10.0, clock=clock
        )
        rerun = self.gated(scheduler, "R", Role.RERUN)
        live = self.gated(scheduler, "L", Role.LIVE)
        self.assertStarted(rerun)
        self.assertStarted(live)
        live_full = self.gated(scheduler, "LF", Role.LIVE, JobType.FULL)
        clock.advance(20.0)
        rerun_quick = self.gated(scheduler, "RQ", Role.RERUN, JobType.QUICK)

        rerun.release.set()
        self.assertStarted(rerun_quick)
        self.assertNotStarted(live_full)

    def test_k_zero_degenerates_to_submission_order(self) -> None:
        scheduler, _, blocker = self.blocked(k=0.0)
        full = self.gated(scheduler, "F", Role.LIVE, JobType.FULL)
        quick = self.gated(scheduler, "Q", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assertStarted(full)
        self.assertNotStarted(quick)

    def test_many_waiting_jobs_all_complete(self) -> None:
        # O4 smoke test: 10k waiting jobs, submit + pick must cope.
        scheduler = self.make_scheduler(total=4, rerun_max=2)
        lock = threading.Lock()
        completed = 0

        def count() -> None:
            nonlocal completed
            with lock:
                completed += 1

        for i in range(10_000):
            scheduler.submit(
                f"J{i}",
                Role.RERUN if i % 3 == 0 else Role.LIVE,
                JobType.FULL if i % 2 else JobType.QUICK,
                count,
            )
        scheduler.shutdown()
        self.assertEqual(completed, 10_000)

    def test_clock_and_k_validation(self) -> None:
        with self.assertRaises(ValueError):
            Scheduler(total=1, rerun_max=1, starvation_after=-1)
        # An infinite (or NaN) K would silently disable O2: a full
        # job could wait forever behind newly arriving quick jobs.
        with self.assertRaises(ValueError):
            Scheduler(total=1, rerun_max=1, starvation_after=float("inf"))
        with self.assertRaises(ValueError):
            Scheduler(total=1, rerun_max=1, starvation_after=float("nan"))
        with self.assertRaises(ValueError):
            Scheduler(total=1, rerun_max=1, clock=42)  # type: ignore[arg-type]

        # Individual samples are validated too: a NaN/inf sample
        # would silently stop full jobs from ever aging.
        now = [float("nan")]
        scheduler = self.make_scheduler(total=1, rerun_max=1, clock=lambda: now[0])
        ran = threading.Event()
        with self.assertRaises(ValueError):
            scheduler.submit("a", Role.LIVE, JobType.QUICK, ran.set)
        now[0] = float("inf")
        with self.assertRaises(ValueError):
            scheduler.submit("a", Role.LIVE, JobType.QUICK, ran.set)
        now[0] = 0.0
        scheduler.submit("a", Role.LIVE, JobType.QUICK, ran.set)
        self.assertFired(ran)


if __name__ == "__main__":
    unittest.main()
