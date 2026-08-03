## v0.5.0 (2026-08-03)

### Feat

- remove haters mode
- add Android WebView client for the web UI
- two progress bars + editable channel list on the web UI
- trigger Telegram comment collection from the web UI

### Fix

- **ci**: push only release tag
- **ci**: publish release changes via branch
- **ci**: trust release checkout directory
- **ci**: install git for release workflow
- apply external review findings on Android WebView client
- serialize channels.json writes, fix error-state progress bar
- pyrogram workdir under uvicorn + job registry hardening
- tighten telegram workflow type hints

### Refactor

- simplify telegram workflows

## v0.4.0 (2026-08-01)

### Feat

- add user comment workflows and channel discovery
- add user profile web UI

### Fix

- harden user profile web UI

## v0.3.0 (2026-02-15)

### Feat

- added commitzen
- mass refactor collector

### Refactor

- common input arg db
- less functions in utils
- minor improvements

## v0.2.0 (2026-02-15)

### Feat

- minor improvements
- async to list channels

### Refactor

- fixed versions of the packages
- remove useless code
- minor improvements (#6)

## v0.1.0 (2026-02-01)

### Feat

- topic removed (#3)
- removed usless database file (#2)

### Fix

- fixed storage structure (#4)

### Refactor

- rename discussion id

## v0.0.1 (2026-01-30)

### Feat

- more channels
- added analytics
- fixed issues with remi meisner
- added arg collect and haters
- split cmds
- added search of haters
- dor
- added upsert user
- more channels
- db name changed
- added channel
- added web for users
- plus logger
- added env
- optimized request - one request for messages
- collector created
- reader works well
- works with docker

### Refactor

- minor improvements
