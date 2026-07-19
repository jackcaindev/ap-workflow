from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.schemas.invoice_job import InvoiceJobEnvelope


@dataclass(frozen=True)
class QueueNames:
    namespace: str
    stream: str
    group: str
    dead_letter_stream: str
    metadata_prefix: str


@pytest.fixture
def queue_names() -> QueueNames:
    namespace = f"freight-ap:integration:{uuid4().hex}"
    return QueueNames(
        namespace=namespace,
        stream=f"{namespace}:jobs",
        group=f"{namespace}:workers",
        dead_letter_stream=f"{namespace}:dlq",
        metadata_prefix=f"{namespace}:metadata",
    )


@pytest.fixture
def worker_settings(queue_names: QueueNames) -> SimpleNamespace:
    return SimpleNamespace(
        INVOICE_STREAM=queue_names.stream,
        INVOICE_CONSUMER_GROUP=queue_names.group,
        INVOICE_DEAD_LETTER_STREAM=queue_names.dead_letter_stream,
        INVOICE_METADATA_PREFIX=queue_names.metadata_prefix,
        INVOICE_MAX_ATTEMPTS=3,
        INVOICE_VISIBILITY_TIMEOUT_MS=100,
    )


@pytest_asyncio.fixture
async def redis_client(queue_names: QueueNames) -> Redis:
    client = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    assert await client.ping()
    try:
        yield client
    finally:
        try:
            keys = [key async for key in client.scan_iter(f"{queue_names.namespace}*")]
            if keys:
                await client.delete(*keys)
        except RedisError:
            # Preserve the primary assertion and its container diagnostics if a
            # restart test leaves Redis unavailable.
            pass
        await client.aclose()


@pytest.fixture
def make_integration_job() -> Callable[[str], InvoiceJobEnvelope]:
    def _make(suffix: str) -> InvoiceJobEnvelope:
        content = f"integration-content-{suffix}".encode()
        return InvoiceJobEnvelope(
            idempotency_key=hashlib.sha256(f"integration-key-{suffix}".encode()).hexdigest(),
            gmail_account="ap-integration@example.com",
            message_id=f"message-{suffix}",
            gmail_thread_id=f"gmail-thread-{suffix}",
            mime_part_id=f"part-{suffix}",
            filename=f"invoice-{suffix}.pdf",
            file_bytes=base64.b64encode(content).decode("ascii"),
            content_sha256=hashlib.sha256(content).hexdigest(),
            enqueued_at=datetime.now(UTC),
        )

    return _make


@pytest.fixture
def wait_for_condition() -> Callable[..., Awaitable[object]]:
    async def _wait(
        probe: Callable[[], Awaitable[object]],
        *,
        timeout: float = 10.0,
        interval: float = 0.02,
        description: str = "condition",
    ) -> object:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        last_value: object = None
        while loop.time() < deadline:
            last_value = await probe()
            if last_value:
                return last_value
            await asyncio.sleep(min(interval, max(0, deadline - loop.time())))
        raise AssertionError(f"Timed out waiting for {description}; last value={last_value!r}")

    return _wait


@pytest.fixture
def run_recovery_process():
    helper = Path(__file__).parent / "support" / "recovery_process.py"

    async def _run(
        mode: str,
        *arguments: str,
        expected_exit: int = 0,
        timeout: float = 20.0,
    ) -> tuple[str, str]:
        repo_dir = Path(__file__).parents[2]
        child_env = os.environ.copy()
        child_env["PYTHONPATH"] = os.pathsep.join(
            value
            for value in (str(repo_dir), child_env.get("PYTHONPATH"))
            if value
        )
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(helper),
            mode,
            *arguments,
            cwd=repo_dir,
            env=child_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(), timeout=timeout
            )
        except TimeoutError:
            process.kill()
            await process.wait()
            raise AssertionError(f"Recovery subprocess {mode!r} timed out") from None

        stdout = stdout_bytes.decode()
        stderr = stderr_bytes.decode()
        assert process.returncode == expected_exit, (
            f"Recovery subprocess {mode!r} exited {process.returncode}, "
            f"expected {expected_exit}.\nstdout:\n{stdout}\nstderr:\n{stderr}"
        )
        return stdout, stderr

    return _run
