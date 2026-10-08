## v0.7.0 (2026-10-08)

### Feat

- **web**: move frontend to React, Vite and shadcn/ui (#14)
- implement ruBERT regressor for political coordinates
- **web**: add political coordinate analysis via DeepSeek API

## v0.6.0 (2026-08-04)

### Feat

- **web**: add hourly and daily activity bar charts to user profile
- **web**: dynamic daily chart with wheel zoom and year separators

### Fix

- removed useless fields
- **web**: guard against empty activity, short dates, and label misalignment
- **web**: replace JS wheel zoom with natural horizontal scroll
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
