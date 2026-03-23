from __future__ import annotations

import unittest

from alphavault.db.turso_db import is_turso_stream_not_found_error


class TestTursoDb(unittest.TestCase):
    def test_is_turso_stream_not_found_error_true(self) -> None:
        err = ValueError(
            'Hrana: `api error: `status=404 Not Found, body={"error":"stream not found: abc"} ``'
        )
        self.assertTrue(is_turso_stream_not_found_error(err))

    def test_is_turso_stream_not_found_error_true_wrapped(self) -> None:
        inner = ValueError("stream not found: xyz")
        try:
            raise RuntimeError("outer") from inner
        except RuntimeError as outer:
            self.assertTrue(is_turso_stream_not_found_error(outer))

    def test_is_turso_stream_not_found_error_false(self) -> None:
        err = RuntimeError("connection reset by peer")
        self.assertFalse(is_turso_stream_not_found_error(err))


if __name__ == "__main__":
    unittest.main()

