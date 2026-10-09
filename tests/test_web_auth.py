from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from web_auth import PASSWORD, logged_in

from web.app import create_app
from web.auth import (
    COOKIE_NAME,
    LOGIN_ATTEMPTS,
    LOGIN_WINDOW_SECONDS,
    SESSION_SECONDS,
    LoginThrottle,
    issue_token,
    token_is_valid,
)

# Never connected to: these tests stay off the database.
_NO_DATABASE = "postgresql+asyncpg://nobody@127.0.0.1:1/none"

_PROTECTED = [
    ("GET", "/api/v1/profiles"),
    ("GET", "/api/v1/channels"),
    ("POST", "/api/v1/channels"),
    ("POST", "/api/v1/collect"),
    ("GET", "/api/v1/collect/some-job/status"),
    ("POST", "/api/v1/collect/some-job/cancel"),
    ("GET", "/api/v1/users/7"),
    ("GET", "/api/v1/users/7/comments.txt"),
    ("POST", "/api/v1/users/7/political-coords"),
    ("GET", "/api/v1/users/7/position-comparisons"),
    ("GET", "/api/v1/users/7/search?q=x"),
    ("POST", "/api/v1/position-analysis"),
]


def _app(tmp_path: Path, **kwargs):
    return create_app(
        database_url=_NO_DATABASE,
        channels=["chan_a"],
        frontend_dist=tmp_path / "missing",
        **kwargs,
    )


@pytest.mark.parametrize(("method", "url"), _PROTECTED)
def test_api_refuses_a_request_without_a_session(tmp_path: Path, method: str, url: str):
    with TestClient(_app(tmp_path)) as client:
        response = client.request(method, url)

    assert response.status_code == 401


def test_every_api_route_but_the_login_is_protected(tmp_path: Path):
    """A route added later without the session check fails here."""
    app = _app(tmp_path)
    open_paths = {"/api/v1/auth/login", "/api/v1/auth/logout"}
    answers = {}

    with TestClient(app) as client:
        for path, operations in app.openapi()["paths"].items():
            if path in open_paths:
                continue
            url = path.replace("{tg_id}", "7").replace("{job_id}", "some-job")
            for method in operations:
                answers[method, path] = client.request(method, url).status_code

    assert len(answers) >= len(_PROTECTED)
    assert set(answers.values()) == {401}


def test_login_with_the_password_opens_the_api(tmp_path: Path):
    with TestClient(_app(tmp_path)) as client:
        response = client.post("/api/v1/auth/login", json={"password": PASSWORD})

        assert response.status_code == 204
        assert client.get("/api/v1/channels").json() == {"channels": ["chan_a"]}


def test_login_with_a_wrong_password_is_refused(tmp_path: Path):
    with TestClient(_app(tmp_path)) as client:
        response = client.post("/api/v1/auth/login", json={"password": PASSWORD + "x"})

        assert response.status_code == 401
        assert "set-cookie" not in response.headers
        assert client.get("/api/v1/channels").status_code == 401


def test_session_cookie_is_hidden_from_scripts_and_other_sites(tmp_path: Path):
    with TestClient(_app(tmp_path)) as client:
        response = client.post("/api/v1/auth/login", json={"password": PASSWORD})

    cookie = response.headers["set-cookie"].lower()
    assert cookie.startswith(f"{COOKIE_NAME}=")
    assert "httponly" in cookie
    assert "samesite=strict" in cookie
    assert "path=/" in cookie
    assert f"max-age={SESSION_SECONDS}" in cookie
    assert "secure" not in cookie


def test_session_cookie_is_secure_over_https(tmp_path: Path):
    with TestClient(_app(tmp_path), base_url="https://testserver") as client:
        response = client.post("/api/v1/auth/login", json={"password": PASSWORD})

    assert "secure" in response.headers["set-cookie"].lower()


def test_logout_closes_the_api(tmp_path: Path):
    with logged_in(_app(tmp_path)) as client:
        assert client.get("/api/v1/channels").status_code == 200

        response = client.post("/api/v1/auth/logout")

        assert response.status_code == 204
        assert client.get("/api/v1/channels").status_code == 401


@pytest.mark.parametrize(
    "cookie",
    [
        "",
        "garbage",
        "not-a-number.abcdef",
        issue_token("another-password"),
        issue_token(PASSWORD)[:-1],
    ],
)
def test_api_refuses_a_forged_cookie(tmp_path: Path, cookie: str):
    with TestClient(_app(tmp_path), cookies={COOKIE_NAME: cookie}) as client:
        assert client.get("/api/v1/channels").status_code == 401


def test_token_expires():
    token = issue_token(PASSWORD, now=1000)

    assert token_is_valid(PASSWORD, token, now=1000 + SESSION_SECONDS - 1)
    assert not token_is_valid(PASSWORD, token, now=1000 + SESSION_SECONDS)


def test_token_expiry_cannot_be_extended():
    expires, signature = issue_token(PASSWORD, now=1000).split(".")
    later = f"{int(expires) + 1}.{signature}"

    assert not token_is_valid(PASSWORD, later, now=1000)


def test_token_of_another_password_is_refused():
    assert not token_is_valid(PASSWORD, issue_token(PASSWORD + "x", now=1000), now=1000)


def test_repeated_wrong_passwords_block_the_login(tmp_path: Path):
    with TestClient(_app(tmp_path)) as client:
        for _ in range(LOGIN_ATTEMPTS):
            wrong = client.post("/api/v1/auth/login", json={"password": "wrong"})
            assert wrong.status_code == 401

        blocked = client.post("/api/v1/auth/login", json={"password": PASSWORD})

    assert blocked.status_code == 429
    assert 0 < int(blocked.headers["retry-after"]) <= LOGIN_WINDOW_SECONDS
    assert "set-cookie" not in blocked.headers


def test_a_successful_login_forgets_the_wrong_attempts(tmp_path: Path):
    with TestClient(_app(tmp_path)) as client:
        for _ in range(LOGIN_ATTEMPTS - 1):
            client.post("/api/v1/auth/login", json={"password": "wrong"})
        assert (
            client.post("/api/v1/auth/login", json={"password": PASSWORD}).status_code
            == 204
        )

        for _ in range(LOGIN_ATTEMPTS - 1):
            client.post("/api/v1/auth/login", json={"password": "wrong"})
        again = client.post("/api/v1/auth/login", json={"password": PASSWORD})

    assert again.status_code == 204


def test_throttle_counts_each_address_by_itself():
    throttle = LoginThrottle(attempts=2, window=60, clock=lambda: 0.0)
    throttle.failed("a")
    throttle.failed("a")

    assert throttle.retry_after("a") == 60
    assert throttle.retry_after("b") == 0


def test_throttle_lets_the_address_in_after_the_window():
    now = [0.0]
    throttle = LoginThrottle(attempts=2, window=60, clock=lambda: now[0])
    throttle.failed("a")
    now[0] = 30.0
    throttle.failed("a")

    assert throttle.retry_after("a") == 30
    now[0] = 59.0
    assert throttle.retry_after("a") == 1
    now[0] = 60.0
    assert throttle.retry_after("a") == 0


def test_throttle_forgets_addresses_whose_attempts_expired():
    now = [0.0]
    throttle = LoginThrottle(attempts=2, window=60, clock=lambda: now[0])
    throttle.failed("a")
    now[0] = 61.0
    throttle.failed("b")

    assert throttle.tracked() == 1


@pytest.mark.parametrize("password", ["", None])
def test_app_does_not_start_without_a_password(tmp_path: Path, monkeypatch, password):
    monkeypatch.delenv("WEB_PASSWORD", raising=False)

    with pytest.raises(RuntimeError, match="WEB_PASSWORD"):
        _app(tmp_path, password=password)


def test_password_argument_wins_over_the_environment(tmp_path: Path):
    with TestClient(_app(tmp_path, password="from-argument")) as client:
        assert (
            client.post("/api/v1/auth/login", json={"password": PASSWORD}).status_code
            == 401
        )
        assert (
            client.post(
                "/api/v1/auth/login", json={"password": "from-argument"}
            ).status_code
            == 204
        )


def test_login_page_is_served_without_a_session(tmp_path: Path):
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html>app</html>")
    app = create_app(database_url=_NO_DATABASE, channels=[], frontend_dist=dist)

    with TestClient(app) as client:
        response = client.get("/login")

    assert response.status_code == 200
    assert response.text == "<html>app</html>"
