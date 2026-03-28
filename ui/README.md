# ANBU Django UI

This folder contains the Django migration of the Streamlit interface.

## Stack
- Python 3.12
- Django 6.0.2
- HTMX + Bootstrap
- Bootstrap tables
- Ace (SQL editor, vendored in `ui/static/vendor/ace`)

## Database layout
- `default` (Postgres): Django auth/admin/session tables + audit.
- `data_store` (Postgres): runtime/result table `anbu_result`.
- `rule_audit` (renamed to `target_audit`) is in `default`.
- `rules/` directory: canonical target persistence store (`<TARGET>.env` + `<TARGET>.sql`).
- UI bootstrap/application config lives in `ui/.env`.
- Datasource/action definition files are edited in place from the paths declared by `DATASOURCE_DEFINITION_FILE` and `ACTION_DEFINITION_FILE` in `ui/.env`.

## Run
1. Install dependencies:
   - `py -3 -m pip install -r requirements.txt`
2. Configure `ui/.env`.
   - The default file is engine-compatible and points to `eva/datasources.properties` and `eva/actions.properties`.
   - PostgreSQL bootstrap values come from `PG_HOST`, `PG_PORT`, `PG_DBNAME`, `PG_USER`, `PG_PASS`.
3. Create Django tables on PostgreSQL:
   - `py -3 ui\manage.py migrate`
4. Create admin user:
   - `py -3 ui\manage.py createsuperuser`
5. Start server:
   - `py -3 ui\manage.py runserver`

## Routes
- `/settings/` : main page (`ANBU Settings`)
- `/settings/datasources/` : data source list
- `/settings/datasources/new/` : add data source
- `/settings/datasources/<NAME>/edit/` : edit data source
- `/settings/actions/` : action properties list
- `/settings/actions/new/` : add action
- `/settings/actions/<NAME>/edit/` : edit action
- `/targets/` : target rules page (`Targets`)
- `/targets/new/` : create target
- `/targets/<TARGET>/edit/` : edit target
- `/targets/<TARGET>/` : target detail/history/instances
- `/explore/` : read-only SQL explore
- `/admin/` : user management (PostgreSQL auth tables)

## Offline note
- Ace is served from local static files (no CDN dependency for editor).
