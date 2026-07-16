import asyncio
import json

import pytest
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.services import invoice_worker


class FakeRedis:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


async def _run_worker_until_first_summary(
    monkeypatch: pytest.MonkeyPatch,
    raw_payloads: list[str],
) -> tuple[list[dict], FakeRedis]:
    redis = FakeRedis()
    drain_calls = 0
    summaries: list[list[dict]] = []

    async def fake_drain_batch(*_args, **_kwargs) -> list[str]:
        nonlocal drain_calls
        drain_calls += 1
        if drain_calls == 1:
            return raw_payloads
        if drain_calls == 2:
            return []
        raise asyncio.CancelledError

    async def fake_flush_batch_summary(results: list[dict]) -> list[dict]:
        summaries.append(list(results))
        return []

    monkeypatch.setattr(
        invoice_worker.Redis,
        "from_url",
        lambda *_args, **_kwargs: redis,
    )
    monkeypatch.setattr(invoice_worker, "_drain_batch", fake_drain_batch)
    monkeypatch.setattr(
        invoice_worker,
        "_flush_batch_summary",
        fake_flush_batch_summary,
    )

    with pytest.raises(asyncio.CancelledError):
        await invoice_worker.process_invoice_queue()

    assert len(summaries) == 1
    return summaries[0], redis


async def test_worker_processes_multiple_jobs_in_one_batch_summary(monkeypatch):
    jobs = [
        {"filename": "invoice-a.pdf", "file_bytes": "YQ=="},
        {"filename": "invoice-b.pdf", "file_bytes": "Yg=="},
        {"filename": "invoice-c.pdf", "file_bytes": "Yw=="},
    ]

    async def fake_run_workflow_for_job(job: dict) -> dict:
        return invoice_worker._summary_result(
            filename=job["filename"],
            run_id=f"run-{job['filename']}",
            status="complete",
        )

    monkeypatch.setattr(
        invoice_worker,
        "_run_workflow_for_job",
        fake_run_workflow_for_job,
    )

    summary, redis = await _run_worker_until_first_summary(
        monkeypatch,
        [json.dumps(job) for job in jobs],
    )

    assert {result["filename"] for result in summary} == {
        "invoice-a.pdf",
        "invoice-b.pdf",
        "invoice-c.pdf",
    }
    assert all(result["status"] == "complete" for result in summary)
    assert redis.closed is True


async def test_one_batch_job_failure_does_not_block_siblings(monkeypatch):
    jobs = [
        {"filename": "invoice-a.pdf", "file_bytes": "YQ=="},
        {"filename": "broken.pdf", "file_bytes": "Yg=="},
        {"filename": "invoice-c.pdf", "file_bytes": "Yw=="},
    ]

    async def fake_run_workflow_for_job(job: dict) -> dict:
        if job["filename"] == "broken.pdf":
            raise RuntimeError("branch failed")
        return invoice_worker._summary_result(
            filename=job["filename"],
            run_id=f"run-{job['filename']}",
            status="complete",
        )

    monkeypatch.setattr(
        invoice_worker,
        "_run_workflow_for_job",
        fake_run_workflow_for_job,
    )

    summary, _ = await _run_worker_until_first_summary(
        monkeypatch,
        [json.dumps(job) for job in jobs],
    )
    by_filename = {result["filename"]: result for result in summary}

    assert by_filename["invoice-a.pdf"]["status"] == "complete"
    assert by_filename["invoice-c.pdf"]["status"] == "complete"
    assert by_filename["broken.pdf"]["status"] == "failed"
    assert by_filename["broken.pdf"]["exception_reason"] == "branch failed"


async def test_drain_batch_caps_number_of_popped_jobs():
    class QueueRedis:
        def __init__(self) -> None:
            self.remaining = ["second", "third", "fourth"]
            self.rpop_calls = 0

        async def brpop(self, queue: str, timeout: int):
            assert queue == invoice_worker.INVOICE_QUEUE
            assert timeout == 5
            return queue, "first"

        async def rpop(self, queue: str):
            assert queue == invoice_worker.INVOICE_QUEUE
            self.rpop_calls += 1
            return self.remaining.pop(0) if self.remaining else None

    redis = QueueRedis()

    payloads = await invoice_worker._drain_batch(
        redis,
        max_batch_size=3,
        wait_timeout=5,
    )

    assert payloads == ["first", "second", "third"]
    assert redis.rpop_calls == 2
    assert redis.remaining == ["fourth"]


async def test_redis_socket_timeout_uses_worker_timeout_path(monkeypatch):
    class TimeoutRedis(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.brpop_calls = 0

        async def brpop(self, _queue: str, timeout: int):
            assert timeout == 5
            self.brpop_calls += 1
            if self.brpop_calls == 1:
                raise RedisTimeoutError("socket timeout")
            raise asyncio.CancelledError

    redis = TimeoutRedis()
    flush_calls = 0

    async def fake_flush_batch_summary(results: list[dict]) -> list[dict]:
        nonlocal flush_calls
        flush_calls += 1
        return results

    monkeypatch.setattr(
        invoice_worker.Redis,
        "from_url",
        lambda *_args, **_kwargs: redis,
    )
    monkeypatch.setattr(
        invoice_worker,
        "_flush_batch_summary",
        fake_flush_batch_summary,
    )

    with pytest.raises(asyncio.CancelledError):
        await invoice_worker.process_invoice_queue()

    assert flush_calls == 1
    assert redis.brpop_calls == 2
    assert redis.closed is True
