from __future__ import annotations

from alphavault.db.turso_db import is_turso_stream_not_found_error


def test_is_turso_stream_not_found_error_true() -> None:
    err = ValueError(
        'Hrana: `api error: `status=404 Not Found, body={"error":"stream not found: abc"} ``'
    )
    assert is_turso_stream_not_found_error(err)


def test_is_turso_stream_not_found_error_true_wrapped() -> None:
    inner = ValueError("stream not found: xyz")
    try:
        raise RuntimeError("outer") from inner
    except RuntimeError as outer:
        assert is_turso_stream_not_found_error(outer)


def test_is_turso_stream_not_found_error_false() -> None:
    err = RuntimeError("connection reset by peer")
    assert not is_turso_stream_not_found_error(err)
