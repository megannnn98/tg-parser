"""A signed-in test client: every API route but the login needs the session cookie."""

from fastapi.testclient import TestClient

from web.auth import COOKIE_NAME, issue_token

PASSWORD = "test-password"


def logged_in(app, **kwargs) -> TestClient:
    client = TestClient(app, **kwargs)
    response = client.post("/api/v1/auth/login", json={"password": PASSWORD})
    assert response.status_code == 204, response.text
    return client


def session_cookies() -> dict[str, str]:
    """For a client that cannot go through the login, such as httpx.AsyncClient."""
    return {COOKIE_NAME: issue_token(PASSWORD)}
