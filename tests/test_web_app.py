from pathlib import Path

import pytest
from fastapi import HTTPException

from web.app import _resolve_user_db


def test_resolve_user_db_allows_direct_db_file(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    db_path.touch()

    assert _resolve_user_db(tmp_path, "vasya_7.db") == db_path.resolve()


@pytest.mark.parametrize(
    "db_name",
    [
        "../vasya_7.db",
        "nested/vasya_7.db",
        "/tmp/vasya_7.db",
        "vasya_7.sqlite",
    ],
)
def test_resolve_user_db_rejects_unsafe_names(tmp_path: Path, db_name: str):
    with pytest.raises(HTTPException) as exc_info:
        _resolve_user_db(tmp_path, db_name)

    assert exc_info.value.status_code == 404
