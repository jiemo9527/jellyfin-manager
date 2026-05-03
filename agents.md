# AGENTS PLAYBOOK (jellyfin-manager)

This document is the working contract for engineering agents and maintainers.
It is intentionally detailed and should be kept in sync with real code behavior.

## 1. Mission

`jellyfin-manager` is a FastAPI + SQLite control plane for Jellyfin operations:

- user lifecycle (create, disable, enable, extend, plan changes, delete)
- audit/sync unmanaged users
- split-routing ban rules (including StartJ-backed source pools)
- server traffic monitor integration
- media library scan triggers
- backup + restore tooling
- device cleanup tooling
- Telegram admin bot and notification bot

The web process also hosts several background schedulers and starts the ban worker.

## 2. High-Level Architecture

- Web app: `jm_manager/app.py` (FastAPI, templates, API routes, schedulers)
- DB layer: `jm_manager/db.py` + store modules (`users_store.py`, `ban_rules_store.py`)
- Runtime settings: `jm_manager/runtime_settings.py` (`app_settings` key/value)
- Jellyfin client: `jm_manager/jellyfin_api.py`
- Split-routing worker: `jm_manager/banuser_worker.py` (thread started by web app)
- StartJ pool ingestion/cache: `jm_manager/startj_pools.py`
- Backup runtime: `jm_manager/backup.py`
- Telegram admin bot service: `jm_manager/telegram_admin_bot.py`
- Telegram notifications: `jm_manager/telegram_notify.py`

## 3. Repo Map

- App package: `jm_manager/`
- Templates: `templates/`
- Static assets: `static/`
- Container/runtime packaging: `deploy/`
- Runtime data (local/dev): `data/` (mounted as `/data` in containers)
- Main env sample: `.env.example`

## 4. Process Model and Startup

### 4.1 Web container

- Entrypoint: `deploy/entrypoint.sh`
- Default command: `python -m jm_manager` (see `jm_manager/__main__.py`)
- Uvicorn host/port from env:
  - `JM_HOST` (default `0.0.0.0`)
  - `JM_PORT` (default `18080`)

### 4.2 Startup side effects (`app.py`)

On FastAPI startup (`_startup()`), the app:

1. initializes `app.state` caches/status
2. initializes DB schema (`init_db`)
3. starts schedulers:
   - backup scheduler
   - user lifecycle scheduler
   - device cleanup scheduler
   - StartJ pool refresh scheduler
   - stream usage check scheduler
4. starts ban worker via `start_banuser_worker(settings.db_path)`

### 4.3 Data path normalization invariant

`deploy/entrypoint.sh` enforces `/app/data -> /data` symlink.
This keeps relative `data/...` paths aligned with mounted persistent storage.

Critical persisted files:

- DB: `/data/jellyfin_manager.db`
- ban worker log: `/data/banuser.log`

## 5. Data Model and Persistence

SQLite schema is in `jm_manager/db.py` (`SCHEMA_SQL`):

- `users`
- `app_settings`
- `ban_user_blacklists`
- `ban_user_overrides`

Rules of thumb:

- user records are canonical in `users`
- mutable app/runtime config is canonical in `app_settings`
- split-routing applied snapshot is canonical in `ban_user_blacklists`

## 6. Configuration Model

Two layers exist:

1. **Process env** (boot-level): host/port/db/session secret
2. **Runtime settings** (`app_settings` table): operational controls

Runtime settings are defined in `RuntimeSettings` (`jm_manager/runtime_settings.py`).

## 7. Current UI Scope Contract (`/settings`)

`/settings` uses scope-based card switching.
Current active scopes are:

- `jellyfin` (includes Jellyfin + Security sections)
- `telegram`
- `library_scan`
- `backup` (includes Backup + DB maintenance + import/export sections)
- `schedules`
- `stream`

Compatibility mapping in backend/UI logic:

- `security` -> `jellyfin`
- `maintenance` -> `backup`

The old `all` scope/card is removed.

## 8. StartJ and Split-Routing Contract

### 8.1 Source of truth

- StartJ URL is runtime-configured (`startj_url` in `app_settings`)
- fetched pool snapshot cached in `startj_pools_json` (in `app_settings`)
- effective split-routing rules consumed by worker come from `ban_user_blacklists`

### 8.2 Automatic refresh/sync

Scheduler in `app.py` runs every 60s and calls `refresh_startj_pools(..., ttl_seconds=300)`.
Effective pull cadence is up to every 5 minutes.

If pool content changed, app syncs new effective blacklists to `ban_user_blacklists`.
When StartJ changes URL domains, saved split-routing rules are migrated by comparing the old cached StartJ pool with the new StartJ pool and mapping matching subdomain prefixes to the new full URL. Jellyfin Base URL / Pro URL changes must not drive split-routing URL migration; StartJ is the only source of routing endpoint URLs.

## 9. Server Stream Save Contract

Stream server JSON save behavior is merge-first, not destructive replace:

- incoming JSON merges into existing list
- dedupe key is `veid`
- latest entry for same `veid` wins

Single-item delete endpoint exists:

- `POST /settings/stream-servers/delete`

## 10. Media Library Scan Contract

### 10.1 Dashboard UX

- dashboard scan trigger is in-place (no redirect to logs page)
- request result is shown as toast bubble
- no full-library quick button in dashboard

### 10.2 Active scan modes (only two)

Defined in `SCAN_MODE_PRESETS` (`app.py`):

1. `default` (scan new content)
   - `/Refresh?Recursive=true&ImageRefreshMode=Default&MetadataRefreshMode=Default&ReplaceAllImages=false&RegenerateTrickplay=false&ReplaceAllMetadata=false`

2. `missing_and_images` (scan missing content and refresh images)
   - `/Refresh?Recursive=true&ImageRefreshMode=FullRefresh&MetadataRefreshMode=FullRefresh&ReplaceAllImages=true&RegenerateTrickplay=true&ReplaceAllMetadata=false`

### 10.3 Telegram admin bot parity

Telegram admin bot mirrors the same two scan modes:

- select library (`scan_pick:{code}`)
- then select mode (`scan_mode:{code}:{mode}`)

The old "scan all libraries" menu path is removed.

## 11. Device Cleanup Contract

- route: `/device-cleanup`
- page title and nav label: `Manual Device Cleanup`
- supports preview + execute
- supports schedule execution and rule-based schedule rows

## 12. Telegram Notification Contract

- Sensitive and non-sensitive Telegram notification channels share one Bot Token (`telegram_bot_token`).
- Each channel has its own recipient list:
  - sensitive: `telegram_user_id`
  - non-sensitive: `telegram_public_user_id`
- Both channels expose the same notification type list in `/settings`; only the checked types differ per channel.
- Empty persisted notification type settings mean legacy-compatible "send all". The explicit sentinel `__none__` means send none.
- User creation notifications include the recommended client line:
  - `📱 推荐客户端：安卓yamby、vidhub/苹果infuse、senplayer/Win 小幻影视、hills-lite`

## 13. UI Dialog Contract

- Do not use browser-native `alert`, `confirm`, or `prompt` in templates.
- Use the shared Web modal helpers from `templates/base.html`:
  - `jmConfirm(message, options)` for confirmations
  - `jmAlert(message, options)` for informational/error dialogs

## 14. Docker Version Display Contract

- Production compose may pull `wanxve0000/jellyfin-manager:latest` directly.
- The Web UI version label (`jfmanager ...`) is driven by the process env `JM_IMAGE_TAG`.
- Release builds should pass `--build-arg JM_IMAGE_TAG=<dockerhub-tag>` and deploy with `JM_IMAGE_TAG=<dockerhub-tag>` in the runtime environment, even if compose pulls `latest`.

## 15. Hidden UI Scope

- The placeholder API management navigation entry is intentionally hidden until a real implementation exists.

## 16. Routing Surface (Grouped)

### UI pages

- `/` dashboard
- `/settings`, `/users`, `/audit`, `/ban-rules`, `/tasks`, `/server-stream`, `/device-cleanup`

### UI actions

- `/users/*` lifecycle actions
- `/audit/*` sync actions
- `/ban-rules/*` save/monitor/log clear actions
- `/tasks/*` sync/scan/backup/schedule actions
- `/settings/*` save/import/export/db-vacuum/stream-delete actions

### API routes

- `/api/info`, `/api/plans`
- `/api/users*`
- `/api/audit*`
- `/api/ban/config`
- `/api/server-stream`
- `/api/tasks/*`

All `/api/*` routes require `X-API-Key` matching runtime `api_key`.

## 17. Local Development

### 13.1 Quick start (host)

1. copy `.env.example` -> `.env`
2. install deps: `pip install -r requirements.txt`
3. run app: `python -m jm_manager`

### 13.2 Telegram admin bot (optional local)

- `python -u -m jm_manager.telegram_admin_bot`

Requires runtime API key + Telegram bot settings configured in `/settings`.

## 18. Docker Development and Deployment

### 14.1 Compose (repo local)

- file: `deploy/docker-compose.yml`
- mounts:
  - `../data:/data`
  - `/srv/jellyfin:/srv/jellyfin` (host-specific)

### 14.2 Build image

- `docker build -f deploy/Dockerfile -t <repo>:<tag> .`

Pass `--build-arg JM_IMAGE_TAG=<tag>` when building release images so the UI version matches the DockerHub tag.

### 14.3 Multi-arch push pattern

- use `docker buildx build --platform linux/amd64,linux/arm64 ... --push`

## 19. Validation Checklist Before Release

Minimum required checks:

1. Python compile passes:
   - `python -m compileall jm_manager`
2. changed templates render without Jinja errors (scripted render)
3. route behavior sanity-check for touched pages/actions
4. if background workers affected, verify logs and DB side effects
5. if Docker deploy affected, verify mounted `/data` persistence behavior

## 20. Security and Safety Rules

- never commit secrets (`.env`, API keys, bot tokens, restic passwords)
- keep clear-on-save behavior explicit (checkbox-driven)
- preserve admin safety behavior in split-routing logic
- avoid destructive user operations without explicit UI/API confirmation path

## 21. Known Operational Invariants

- `banuser_worker` consumes `ban_user_blacklists`, not raw StartJ pool JSON directly
- StartJ updates must eventually sync into blacklist snapshot to affect active routing
- split-routing URL migration follows StartJ old/new pool diffs, never Jellyfin Base/Pro URL edits
- web and bot should use same `JM_DB_PATH`-derived data root semantics
- `latest` tags are mutable; keep `JM_IMAGE_TAG` set to the intended release tag for UI/version visibility

## 22. Agent Editing Guidelines (Project-Specific)

When modifying this repository:

1. keep UX language and workflow consistent with existing Chinese admin UI
2. preserve settings scope compatibility when merging/splitting tabs
3. for scan mode changes, update all three places together:
   - `SCAN_MODE_PRESETS` in `app.py`
   - dashboard select/options in `templates/dashboard.html`
   - Telegram admin bot callback flow in `telegram_admin_bot.py`
4. for StartJ or split-routing changes, validate:
    - pool refresh path
    - blacklist snapshot sync path
    - saved-rule URL migration from old StartJ pool to new StartJ pool
    - ban worker hot reload effect
5. for persistence path changes, validate both:
    - container path (`/data/...`)
    - host mount path
6. for Telegram notification changes, validate both channels share one Bot Token and expose the same type list
7. for UI confirmation changes, keep browser-native dialogs out of templates

## 23. Suggested Maintenance for This File

Update this document whenever any of the following changes:

- settings scope/card model
- scan mode list or parameter contract
- startup schedulers and intervals
- DB schema or key runtime setting fields
- Docker mount/data path conventions
- Telegram admin bot command flow
- Telegram notification channel/type routing
- StartJ-backed split-routing URL migration rules
- shared UI dialog behavior
