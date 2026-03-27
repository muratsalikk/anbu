# ANBU Django UI

This folder contains the Django migration of the Streamlit interface.

## Stack
- Python 3.12
- Django 6.0.2
- HTMX + Bootstrap
- Bootstrap tables
- Ace (SQL editor, vendored in `ui/static/vendor/ace`)

## Database layout
- `default` (Postgres): Django auth/admin/session tables + audit + property files.
- `data_store` (Postgres): runtime/result table `anbu_result`.
- `rule_audit` (renamed to `target_audit`) is in `default`.
- `rules/` directory: canonical target persistence store (`<TARGET>.env` + `<TARGET>.sql`).
- property files are persisted in PostgreSQL table `property_file` and editable from Django admin:
  - `application.properties`
  - `datasources.properties`
  - `actions.properties`
  - `helper.properties`

## Run
1. Install dependencies:
   - `py -3 -m pip install -r requirements.txt`
2. Set PostgreSQL connection values (`DS_HOST`, `DS_PORT`, `DS_DBNAME`, `DS_USER`, `DS_PASS`) via environment
   or `application.properties`.
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
