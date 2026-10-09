"""One shared password for the whole site.

A login sets a signed cookie; every API route but the login and the logout
needs it. Nothing is stored: the cookie carries its own expiry, and its
signature key is derived from the password, so a new password ends every
session.
"""

from __future__ import annotations

import hashlib
import hmac
import time
from collections.abc import Callable

from fastapi import APIRouter, HTTPException, Request, Response

from web.schemas import LoginRequest

COOKIE_NAME = "session"
SESSION_SECONDS = 30 * 24 * 60 * 60
# Wrong passwords one address may send within the window before it is refused.
LOGIN_ATTEMPTS = 5
LOGIN_WINDOW_SECONDS = 5 * 60
# Seconds since 1970 take ten digits; int() refuses thousands of them.
_MAX_EXPIRES_DIGITS = 12


def _signature(password: str, expires: int) -> str:
    key = hashlib.sha256(b"telegram-comments session:" + password.encode()).digest()
    return hmac.new(key, str(expires).encode(), hashlib.sha256).hexdigest()


def issue_token(password: str, now: float | None = None) -> str:
    expires = int(time.time() if now is None else now) + SESSION_SECONDS
    return f"{expires}.{_signature(password, expires)}"


def token_is_valid(password: str, token: str, now: float | None = None) -> bool:
    # The cookie is whatever the visitor sent: nothing below may raise on it.
    expires_text, _, signature = token.partition(".")
    if not expires_text.isascii() or not expires_text.isdigit():
        return False
    if len(expires_text) > _MAX_EXPIRES_DIGITS:
        return False
    expires = int(expires_text)
    if not hmac.compare_digest(
        signature.encode(), _signature(password, expires).encode()
    ):
        return False
    return (time.time() if now is None else now) < expires


class LoginThrottle:
    """Wrong passwords per client address, kept in memory for one window."""

    def __init__(
        self,
        attempts: int = LOGIN_ATTEMPTS,
        window: float = LOGIN_WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._attempts = attempts
        self._window = window
        self._clock = clock
        self._failures: dict[str, list[float]] = {}

    def retry_after(self, address: str) -> int:
        """Seconds until `address` may try again; 0 when it may now."""
        recent = self._recent(address)
        if len(recent) < self._attempts:
            return 0
        # The oldest of the last `attempts` failures leaves the window first.
        wait = recent[-self._attempts] + self._window - self._clock()
        return max(1, int(wait + 0.999))

    def failed(self, address: str) -> None:
        now = self._clock()
        # Addresses that stopped trying are dropped, so the table stays small.
        for known in list(self._failures):
            if not self._recent(known):
                del self._failures[known]
        self._failures.setdefault(address, []).append(now)

    def succeeded(self, address: str) -> None:
        self._failures.pop(address, None)

    def tracked(self) -> int:
        return len(self._failures)

    def _recent(self, address: str) -> list[float]:
        earliest = self._clock() - self._window
        recent = [at for at in self._failures.get(address, []) if at > earliest]
        if address in self._failures:
            self._failures[address] = recent
        return recent


def require_session(request: Request) -> None:
    token = request.cookies.get(COOKIE_NAME, "")
    if not token_is_valid(request.app.state.password, token):
        raise HTTPException(status_code=401, detail="Not signed in")


# Operation ids are the function names: they name the generated frontend client.
auth_api = APIRouter(
    prefix="/auth", generate_unique_id_function=lambda route: route.name
)


# `async`, with nothing awaited: the handler runs on the event loop, so two
# logins never touch the throttle at once and no lock is needed.
@auth_api.post("/login", status_code=204)
async def login(request: Request, payload: LoginRequest) -> Response:
    state = request.app.state
    address = request.client.host if request.client else "unknown"
    throttle: LoginThrottle = state.login_throttle

    wait = throttle.retry_after(address)
    if wait:
        raise HTTPException(
            status_code=429,
            detail="Too many wrong passwords",
            headers={"Retry-After": str(wait)},
        )
    if not hmac.compare_digest(payload.password.encode(), state.password.encode()):
        throttle.failed(address)
        raise HTTPException(status_code=401, detail="Wrong password")

    throttle.succeeded(address)
    response = Response(status_code=204)
    response.set_cookie(
        COOKIE_NAME,
        issue_token(state.password),
        max_age=SESSION_SECONDS,
        httponly=True,
        samesite="strict",
        secure=request.url.scheme == "https",
    )
    return response


@auth_api.post("/logout", status_code=204)
def logout() -> Response:
    response = Response(status_code=204)
    response.delete_cookie(COOKIE_NAME, httponly=True, samesite="strict")
    return response
