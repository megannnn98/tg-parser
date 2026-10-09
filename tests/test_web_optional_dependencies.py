"""The profile web server must start before local inference dependencies are used."""
import os
from pathlib import Path
import subprocess
import sys


def test_web_starts_without_local_model_packages():
    code = """
import asyncio
import builtins
asyncio.set_event_loop(asyncio.new_event_loop())
original = builtins.__import__
def without_model_packages(name, *args, **kwargs):
    if name.split('.')[0] in {'numpy', 'torch', 'transformers'}:
        raise ModuleNotFoundError('blocked dependency', name=name)
    return original(name, *args, **kwargs)
builtins.__import__ = without_model_packages
from web.app import app
assert not hasattr(app.state.position_jobs.service, 'gateway')
print('web ready')
"""
    result = subprocess.run([sys.executable, "-c", code],
                            cwd=Path(__file__).resolve().parents[1],
                            env={**os.environ, "API_ID": "0"},
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert "web ready" in result.stdout
