"""Backwards-compatible re-export of AISStreamClient.

The canonical implementation now lives in ``src/ingest/client.py``.
"""

from src.ingest.client import AISStreamClient

__all__ = ["AISStreamClient"]
