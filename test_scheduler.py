"""Tests for Parts 1-3 (capacity, ordering, urgency). Run: python3 -m unittest -v

Determinism: blocking jobs block on an Event until the test releases
them, so "which jobs are running" is observed exactly, not raced;
Part 2 aging uses a FakeClock advanced by hand. The only real-time
waits are bounded negative waits ("this must NOT start").
"""

import threading
import unittest

from scheduler import JobType, Role, Scheduler

START_TIMEOUT = 2.0  # generous: a correct scheduler starts jobs at once
NOT_STARTED_TIMEOUT = 0.25  # bounded wait to prove a job stays queued


class BlockingJob:
    """A job that signals it started, then blocks until released."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.started = threading.Event()
        self.release = threading.Event()

    def __call__(self) -> None:
        self.started.set()
        self.release.wait()


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
        self._blocking_jobs: list[BlockingJob] = []
        self._schedulers: list[Scheduler] = []

    def tearDown(self) -> None:
        for blocking_job in self._blocking_jobs:
            blocking_job.release.set()
        for scheduler in self._schedulers:
            scheduler.shutdown()

    def make_scheduler(self, total: int, rerun_max: int, **kwargs) -> Scheduler:
        scheduler = Scheduler(total=total, rerun_max=rerun_max, **kwargs)
        self._schedulers.append(scheduler)
        return scheduler

    def submit_blocking_job(
        self,
        scheduler: Scheduler,
        job_id: str,
        role: Role,
        job_type: JobType = JobType.QUICK,
        urgent: bool = False,
    ) -> BlockingJob:
        job = BlockingJob(job_id)
        self._blocking_jobs.append(job)
        scheduler.submit(job_id, role, job_type, job, urgent=urgent)
        return job

    def assert_event_set(self, event: threading.Event) -> None:
        self.assertTrue(event.wait(START_TIMEOUT), "event should have fired")

    def assert_job_started(self, job: BlockingJob) -> None:
        self.assertTrue(
            job.started.wait(START_TIMEOUT), f"{job.name} should have started"
        )

    def assert_job_not_started(self, job: BlockingJob) -> None:
        self.assertFalse(
            job.started.wait(NOT_STARTED_TIMEOUT),
            f"{job.name} should still be waiting",
        )

    def make_scheduler_with_live_blocker(self, k: float = 10.0):
        clock = FakeClock()
        scheduler = self.make_scheduler(
            total=1, rerun_max=1, starvation_after=k, clock=clock
        )
        blocker = self.submit_blocking_job(scheduler, "BLOCK", Role.LIVE)
        self.assert_job_started(blocker)
        return scheduler, clock, blocker


class StarterScenarios(SchedulerTestCase):
    def test_s1_no_rerun_work_live_uses_all_slots(self) -> None:
        scheduler = self.make_scheduler(total=4, rerun_max=1)
        jobs = [self.submit_blocking_job(scheduler, f"L{i}", Role.LIVE) for i in range(6)]

        for job in jobs[:4]:
            self.assert_job_started(job)
        for job in jobs[4:]:
            self.assert_job_not_started(job)
        self.assertEqual(
            scheduler.stats(),
            {"waiting_live": 2, "waiting_rerun": 0,
             "running_live": 4, "running_rerun": 0},
        )

        # Freeing one slot starts the next waiting job (C6: no waste).
        jobs[0].release.set()
        self.assert_job_started(jobs[4])
        self.assert_job_not_started(jobs[5])

    def test_s2_next_freed_slot_goes_to_rerun(self) -> None:
        scheduler = self.make_scheduler(total=4, rerun_max=1)
        running = [self.submit_blocking_job(scheduler, f"L{i}", Role.LIVE) for i in range(4)]
        for job in running:
            self.assert_job_started(job)

        waiting_live = self.submit_blocking_job(scheduler, "L4", Role.LIVE)
        rerun = self.submit_blocking_job(scheduler, "R0", Role.RERUN)
        self.assert_job_not_started(waiting_live)
        self.assert_job_not_started(rerun)

        # Running jobs are never interrupted (C5); the rerun wins the
        # first freed slot even though L4 waited longer (C4).
        running[0].release.set()
        self.assert_job_started(rerun)
        self.assert_job_not_started(waiting_live)

    def test_s3_one_running_rerun_reserves_only_one_slot(self) -> None:
        # rerun_max=2 but only one rerun exists: live may use the
        # other 3 slots. Reserving both rerun slots would waste one.
        scheduler = self.make_scheduler(total=4, rerun_max=2)
        rerun = self.submit_blocking_job(scheduler, "R0", Role.RERUN)
        self.assert_job_started(rerun)

        jobs = [self.submit_blocking_job(scheduler, f"L{i}", Role.LIVE) for i in range(4)]
        for job in jobs[:3]:
            self.assert_job_started(job)
        self.assert_job_not_started(jobs[3])
        self.assertEqual(scheduler.stats()["running_live"], 3)


class CapacityRules(SchedulerTestCase):
    def test_c2_rerun_cap_holds_even_with_free_slots(self) -> None:
        scheduler = self.make_scheduler(total=4, rerun_max=1)
        jobs = [self.submit_blocking_job(scheduler, f"R{i}", Role.RERUN) for i in range(3)]

        self.assert_job_started(jobs[0])
        self.assert_job_not_started(jobs[1])
        self.assert_job_not_started(jobs[2])
        self.assertEqual(scheduler.stats()["running_rerun"], 1)

    def test_live_gets_capacity_back_once_reruns_drain(self) -> None:
        scheduler = self.make_scheduler(total=2, rerun_max=1)
        rerun = self.submit_blocking_job(scheduler, "R0", Role.RERUN)
        live1 = self.submit_blocking_job(scheduler, "L0", Role.LIVE)
        live2 = self.submit_blocking_job(scheduler, "L1", Role.LIVE)
        self.assert_job_started(rerun)
        self.assert_job_started(live1)
        self.assert_job_not_started(live2)

        # C4 second half + C6: the finish notification alone must let
        # the waiting live job claim the freed slot.
        rerun.release.set()
        self.assert_job_started(live2)

    def test_live_cap_tracks_total_rerun_work(self) -> None:
        # total=5, rerun_max=2, two rerun jobs exist (one running, one
        # waiting): live_cap = 5 - 2 = 3, so exactly 3 live jobs run.
        scheduler = self.make_scheduler(total=5, rerun_max=2)
        rerun1 = self.submit_blocking_job(scheduler, "R0", Role.RERUN)
        live1 = self.submit_blocking_job(scheduler, "L0", Role.LIVE)
        live2 = self.submit_blocking_job(scheduler, "L1", Role.LIVE)
        self.assert_job_started(rerun1)
        self.assert_job_started(live1)
        self.assert_job_started(live2)

        rerun2 = self.submit_blocking_job(scheduler, "R1", Role.RERUN)
        live3 = self.submit_blocking_job(scheduler, "L2", Role.LIVE)
        live4 = self.submit_blocking_job(scheduler, "L3", Role.LIVE)
        self.assert_job_started(rerun2)
        self.assert_job_started(live3)
        self.assert_job_not_started(live4)
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
    def test_base_exception_frees_slot_and_worker_survives(self) -> None:
        # KeyboardInterrupt/SystemExit are BaseException, not
        # Exception. With one worker, a leaked slot or dead worker
        # would stop every following job from ever running.
        scheduler = self.make_scheduler(total=1, rerun_max=1)

        def raise_keyboard_interrupt() -> None:
            raise KeyboardInterrupt

        good_ran = threading.Event()
        scheduler.submit(
            "bad", Role.LIVE, JobType.QUICK, raise_keyboard_interrupt
        )
        scheduler.submit("good", Role.LIVE, JobType.QUICK, good_ran.set)
        self.assert_event_set(good_ran)
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

        def run_concurrent_job(role: Role, rendezvous: bool = False) -> None:
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
            scheduler.submit(f"B{i}", role, JobType.QUICK, lambda r=role: run_concurrent_job(r, True))

        def submit_batch(start: int) -> None:
            for i in range(start, n_other, 6):
                role = Role.RERUN if i % 3 == 0 else Role.LIVE
                scheduler.submit(
                    f"J{i}",
                    role,
                    JobType.QUICK,
                    lambda r=role: run_concurrent_job(r),
                )

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
        gate = self.submit_blocking_job(scheduler, "a", Role.LIVE)
        self.assert_job_started(gate)
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

class Ordering(SchedulerTestCase):
    """Part 2: one slot held by a blocker, so waiting order is exact."""

    def test_o1_quick_runs_before_full_in_same_role(self) -> None:
        scheduler, _, blocker = self.make_scheduler_with_live_blocker()
        full = self.submit_blocking_job(scheduler, "F", Role.LIVE, JobType.FULL)
        quick = self.submit_blocking_job(scheduler, "Q", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assert_job_started(quick)
        self.assert_job_not_started(full)
        quick.release.set()
        self.assert_job_started(full)

    def test_o2_full_aged_exactly_k_blocks_new_quick(self) -> None:
        scheduler, clock, blocker = self.make_scheduler_with_live_blocker(k=10.0)
        full = self.submit_blocking_job(scheduler, "F", Role.LIVE, JobType.FULL)
        clock.advance(10.0)
        quick = self.submit_blocking_job(scheduler, "Q", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assert_job_started(full)
        self.assert_job_not_started(quick)

    def test_promoted_full_merges_by_submission_order(self) -> None:
        # Q1 submitted before F stays ahead of it; Q2 submitted after
        # F does not. Promotion puts F into the quick tier, it does
        # not jump it ahead of every quick job.
        scheduler, clock, blocker = self.make_scheduler_with_live_blocker(k=10.0)
        q1 = self.submit_blocking_job(scheduler, "Q1", Role.LIVE, JobType.QUICK)
        full = self.submit_blocking_job(scheduler, "F", Role.LIVE, JobType.FULL)
        clock.advance(20.0)
        q2 = self.submit_blocking_job(scheduler, "Q2", Role.LIVE, JobType.QUICK)

        blocker.release.set()
        self.assert_job_started(q1)
        self.assert_job_not_started(full)
        q1.release.set()
        self.assert_job_started(full)
        self.assert_job_not_started(q2)

    def test_o3_fifo_within_each_type(self) -> None:
        scheduler, _, blocker = self.make_scheduler_with_live_blocker(k=1000.0)
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

    def test_many_waiting_jobs_all_complete(self) -> None:
        # O4 smoke test: 10k waiting jobs, submit + pick must cope.
        scheduler = self.make_scheduler(total=4, rerun_max=2)
        lock = threading.Lock()
        completed = 0

        def record_completion() -> None:
            nonlocal completed
            with lock:
                completed += 1

        for i in range(10_000):
            scheduler.submit(
                f"J{i}",
                Role.RERUN if i % 3 == 0 else Role.LIVE,
                JobType.FULL if i % 2 else JobType.QUICK,
                record_completion,
            )
        scheduler.shutdown()
        self.assertEqual(completed, 10_000)

class UrgentReruns(SchedulerTestCase):
    """Part 3: urgent is an ordering key within reruns, burst-limited."""

    def test_urgent_beats_normal_for_freed_rerun_slot(self) -> None:
        scheduler, _, blocker = self.make_scheduler_with_live_blocker()
        waiting_live = self.submit_blocking_job(scheduler, "L", Role.LIVE)
        normal = self.submit_blocking_job(scheduler, "RN", Role.RERUN)
        urgent = self.submit_blocking_job(scheduler, "RU", Role.RERUN, urgent=True)
        # No preemption: nothing displaces the running blocker.
        self.assert_job_not_started(urgent)

        blocker.release.set()
        self.assert_job_started(urgent)
        self.assert_job_not_started(normal)
        self.assert_job_not_started(waiting_live)

    def test_burst_limit_forces_normal_and_resets(self) -> None:
        # Exact claim order under a sustained urgent stream: three
        # urgents, then a normal, repeatedly. A strict-priority
        # scheduler would run all seven urgents first.
        scheduler, _, blocker = self.make_scheduler_with_live_blocker()
        order: list[str] = []
        for name in ["N1", "N2"] + [f"U{i}" for i in range(1, 8)]:
            scheduler.submit(
                name,
                Role.RERUN,
                JobType.QUICK,
                lambda n=name: order.append(n),
                urgent=name.startswith("U"),
            )
        blocker.release.set()
        scheduler.shutdown()
        self.assertEqual(
            order, ["U1", "U2", "U3", "N1", "U4", "U5", "U6", "N2", "U7"]
        )

    def test_urgency_respects_rerun_cap(self) -> None:
        # Urgency grants no capacity: rerun_max still binds.
        scheduler = self.make_scheduler(total=4, rerun_max=1)
        jobs = [
            self.submit_blocking_job(scheduler, f"U{i}", Role.RERUN, urgent=True)
            for i in range(3)
        ]
        self.assert_job_started(jobs[0])
        self.assert_job_not_started(jobs[1])
        self.assert_job_not_started(jobs[2])
        self.assertEqual(scheduler.stats()["running_rerun"], 1)
        self.assertEqual(scheduler.stats()["waiting_rerun"], 2)

    def test_type_ordering_inside_urgent_tier(self) -> None:
        scheduler, _, blocker = self.make_scheduler_with_live_blocker()
        ufull = self.submit_blocking_job(scheduler, "UF", Role.RERUN, JobType.FULL, urgent=True)
        uquick = self.submit_blocking_job(scheduler, "UQ", Role.RERUN, JobType.QUICK, urgent=True)

        blocker.release.set()
        self.assert_job_started(uquick)
        self.assert_job_not_started(ufull)
        uquick.release.set()
        self.assert_job_started(ufull)

    def test_urgent_live_submissions_rejected(self) -> None:
        scheduler = self.make_scheduler(total=2, rerun_max=1)
        with self.assertRaises(ValueError):
            scheduler.submit("x", Role.LIVE, JobType.QUICK, lambda: None, urgent=True)
        with self.assertRaises(ValueError):
            scheduler.submit("y", Role.LIVE, JobType.FULL, lambda: None, urgent=True)
        with self.assertRaises(ValueError):
            scheduler.submit("z", Role.RERUN, JobType.QUICK, lambda: None, urgent=1)  # type: ignore[arg-type]

        # Rejected submits consumed no job id.
        ran = threading.Event()
        scheduler.submit("x", Role.LIVE, JobType.QUICK, ran.set)
        self.assert_event_set(ran)

        # Zero-cap edge is unchanged: even urgent reruns are refused.
        no_reruns = self.make_scheduler(total=1, rerun_max=0)
        with self.assertRaises(ValueError):
            no_reruns.submit("u", Role.RERUN, JobType.QUICK, lambda: None, urgent=True)


if __name__ == "__main__":
    unittest.main()
