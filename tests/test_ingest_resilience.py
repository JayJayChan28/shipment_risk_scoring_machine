"""Resilience tests for the AIS ingest client.

These tests do NOT hit the network or AWS. They mock the websocket and S3
client to reproduce, in isolation, the failure modes that explain why the
collector stopped on 2026-05-29 and again on 2026-06-11 instead of running
continuously.

Run from the repo root:
    .venv/bin/python -m unittest tests/test_ingest_resilience.py -v

Each test's docstring states the behaviour it pins down. Tests whose names
start with ``test_BUG_`` document a defect that currently causes silent data
loss or a hard crash — they pass because they assert the *current* (bad)
behaviour, so they act as executable documentation until the client is fixed.
"""

import asyncio
import json
import tempfile
import unittest
from unittest import mock

from src.ingest.fetch_tracking import AISStreamClient

_LOG_DIR = tempfile.mkdtemp(prefix="ais_test_logs_")


class _BreakLoop(BaseException):
    """Sentinel used to escape the client's ``while True`` reconnect loop.

    Subclasses BaseException so the client's catch-all ``except Exception``
    reconnect handler does not swallow it.
    """


class FakeWS:
    """Minimal async-iterable stand-in for a websocket connection."""

    def __init__(self, messages):
        self._messages = list(messages)

    async def send(self, *_args, **_kwargs):
        return None

    def __aiter__(self):
        self._it = iter(self._messages)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


class FakeConnect:
    """Async context manager returned by a mocked ``websockets.connect``."""

    def __init__(self, ws=None, raise_on_enter=None):
        self._ws = ws
        self._raise_on_enter = raise_on_enter

    async def __aenter__(self):
        if self._raise_on_enter is not None:
            raise self._raise_on_enter
        return self._ws

    async def __aexit__(self, *_args):
        return False


def make_client(**overrides):
    """Build a client with boto3 mocked out so __init__ touches no AWS."""
    with mock.patch("src.ingest.fetch_tracking.boto3.client") as boto:
        boto.return_value = mock.MagicMock()
        params = dict(
            ais_api_key="test-key",
            s3_bucket="test-bucket",
            aws_region="us-east-2",
            batch_size=3,
            flush_interval_sec=60,
            reconnect_delay_sec=0,
            log_dir=_LOG_DIR,
        )
        params.update(overrides)
        return AISStreamClient(**params)


def _position_message():
    return json.dumps(
        {
            "MessageType": "PositionReport",
            "Message": {
                "PositionReport": {
                    "UserID": 123456789,
                    "Latitude": 50.1,
                    "Longitude": -1.2,
                    "Sog": 12.3,
                    "Cog": 88.0,
                }
            },
            "MetaData": {"ShipName": "TEST VESSEL"},
        }
    )


class RunLoopResilienceTests(unittest.IsolatedAsyncioTestCase):
    """The ``run`` loop only recovers from a narrow set of exceptions."""

    async def test_unexpected_exception_triggers_reconnect(self):
        """Regression for the original crash: an exception that is NOT
        ConnectionClosed/OSError (e.g. websocket handshake failure, timeout,
        DNS error) must now be caught and reconnected instead of killing the
        process.
        """
        client = make_client(reconnect_delay_sec=0)

        # 1st connect: simulated handshake/timeout. 2nd: sentinel to exit.
        connect = mock.patch(
            "src.ingest.fetch_tracking.websockets.connect",
            side_effect=[RuntimeError("handshake failed (simulated 4xx/timeout)"), _BreakLoop()],
        )
        with connect as connect_mock:
            with self.assertRaises(_BreakLoop):
                await client.run()

        # Reconnected after the unexpected error rather than crashing.
        self.assertEqual(connect_mock.call_count, 2)

    async def test_oserror_triggers_reconnect_not_crash(self):
        """A plain network drop (OSError) IS handled: the loop sleeps and
        reconnects rather than crashing."""
        client = make_client(reconnect_delay_sec=0)

        # 1st connect: network blip. 2nd connect: sentinel to exit the loop.
        connect = mock.patch(
            "src.ingest.fetch_tracking.websockets.connect",
            side_effect=[OSError("connection refused"), _BreakLoop()],
        )
        with connect as connect_mock:
            with self.assertRaises(_BreakLoop):
                await client.run()

        # Two attempts == the reconnect path ran after the OSError.
        self.assertEqual(connect_mock.call_count, 2)

    async def test_size_trigger_flushes_a_full_batch(self):
        """When batch_size messages arrive, exactly one flush happens."""
        client = make_client(batch_size=3)
        messages = [_position_message() for _ in range(3)]

        ws = FakeWS(messages)
        with mock.patch.object(client, "flush_batch") as flush, mock.patch(
            "src.ingest.fetch_tracking.websockets.connect",
            side_effect=[FakeConnect(ws=ws), _BreakLoop()],
        ):
            with self.assertRaises(_BreakLoop):
                await client.run()

        flush.assert_called_once()
        flushed_batch = flush.call_args.args[0]
        self.assertEqual(len(flushed_batch), 3)

    async def test_BUG_buffered_records_below_batch_are_held_in_memory(self):
        """Records below batch_size and within the flush interval stay in RAM.

        If the process is killed (SIGHUP from a closing SSH session, instance
        stop, OOM) while fewer than batch_size messages are buffered, those
        records are lost — explaining the short/partial final files right
        before each outage.
        """
        client = make_client(batch_size=500, flush_interval_sec=60)
        messages = [_position_message() for _ in range(10)]  # < batch_size

        ws = FakeWS(messages)
        with mock.patch.object(client, "flush_batch") as flush, mock.patch(
            "src.ingest.fetch_tracking.websockets.connect",
            side_effect=[FakeConnect(ws=ws), _BreakLoop()],
        ):
            with self.assertRaises(_BreakLoop):
                await client.run()

        # Stream ended cleanly (no ConnectionClosed) -> leftover never flushed.
        flush.assert_not_called()


class FlushResilienceTests(unittest.TestCase):
    """S3 flush error handling currently drops data silently."""

    def test_BUG_s3_failure_silently_drops_the_batch(self):
        """A failed S3 PUT is swallowed with a warning and the batch is lost.

        flush_batch catches Exception, prints '[warn] S3 failed', and returns
        without retrying or buffering — so any S3 hiccup permanently drops
        those records.
        """
        client = make_client()
        client.boto3_client.put_object.side_effect = Exception("throttled")

        batch = [{"mmsi": 1}, {"mmsi": 2}]
        # No exception propagates (data is dropped, not raised).
        self.assertIsNone(client.flush_batch(batch))
        client.boto3_client.put_object.assert_called_once()

    def test_flush_builds_ndjson_payload_and_key(self):
        """Happy path: one PUT, NDJSON body, raw/ais prefix in the key."""
        client = make_client(s3_prefix="raw/ais")
        batch = [{"mmsi": 1, "lat": 50.0}, {"mmsi": 2, "lat": 51.0}]

        client.flush_batch(batch)

        client.boto3_client.put_object.assert_called_once()
        kwargs = client.boto3_client.put_object.call_args.kwargs
        self.assertEqual(kwargs["Bucket"], "test-bucket")
        self.assertTrue(kwargs["Key"].startswith("raw/ais/ais_"))
        self.assertTrue(kwargs["Key"].endswith(".jsonl"))
        # Two records -> two newline-delimited JSON lines.
        lines = kwargs["Body"].decode("utf-8").strip().split("\n")
        self.assertEqual(len(lines), 2)
        self.assertEqual(json.loads(lines[0])["mmsi"], 1)


class NormalizeMessageTests(unittest.TestCase):
    """normalize_message must never crash on bad input."""

    def setUp(self):
        self.client = make_client()

    def test_valid_position_report_is_parsed(self):
        record = self.client.normalize_message(_position_message())
        self.assertIsNotNone(record)
        self.assertEqual(record["message_type"], "PositionReport")
        self.assertEqual(record["mmsi"], 123456789)
        self.assertEqual(record["latitude"], 50.1)

    def test_malformed_json_returns_none(self):
        self.assertIsNone(self.client.normalize_message("{not valid json"))

    def test_empty_message_body_returns_none(self):
        payload = json.dumps({"MessageType": "PositionReport", "Message": {}})
        self.assertIsNone(self.client.normalize_message(payload))


if __name__ == "__main__":
    unittest.main(verbosity=2)
