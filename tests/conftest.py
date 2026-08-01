import fake_pyrogram

# Must run before any test module imports parser.telegram (directly or through
# parser.user_collector / parser.user_finder).
fake_pyrogram.install()
