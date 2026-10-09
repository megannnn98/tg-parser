import os

import fake_pyrogram

# Must run before any test module imports parser.telegram (directly or through
# parser.user_collector / parser.user_finder).
fake_pyrogram.install()

from web_auth import PASSWORD

# The web application does not start without a password.
os.environ.setdefault("WEB_PASSWORD", PASSWORD)
