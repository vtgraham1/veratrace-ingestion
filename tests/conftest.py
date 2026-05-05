"""Shared pytest fixtures.

Builds the MagicMock that `urllib.request.urlopen` returns. Several test files
patch urlopen to inject fake responses; centralizing the mock builder keeps the
context-manager protocol (`__enter__`/`__exit__`) and JSON-encoding consistent.
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_urlopen_response():
    """Factory fixture returning a context-manager MagicMock with .read() → JSON-encoded body.

    Usage:
      def test_x(mock_urlopen_response):
          resp = mock_urlopen_response([{"id": 1}])
          with patch("...urlopen", return_value=resp):
              ...
    """
    def _build(body, status: int = 200) -> MagicMock:
        resp = MagicMock()
        resp.read.return_value = json.dumps(body).encode()
        resp.status = status
        resp.__enter__ = lambda self: resp
        resp.__exit__ = lambda *a: False
        return resp
    return _build
