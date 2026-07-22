# Greece Watch

Read-only Polymarket observer for Greece-related markets. No trading, no financial
side effects — it only reads public Polymarket data and sends a free push
notification (via [ntfy.sh](https://ntfy.sh)) when:

1. A new Greece-related event appears on Polymarket.
2. A new candidate/outcome is added to an event already being tracked (e.g. a new
   name added to the "Next Prime Minister of Greece?" roster).
3. An existing candidate/outcome's YES price swings by 20 percentage points or more
   since the last notification.

Runs on a GitHub Actions schedule (`.github/workflows/greece_watch.yml`, every 20
minutes) — no server, no cost. State is persisted in `greece_watch.sqlite3`,
committed back to the repo after each run.

## Setup

Requires one repository secret: `NTFY_TOPIC` — the ntfy.sh topic name to push to.
Never committed in plain text (this repo is public).
