#!/bin/sh
# The official PostgreSQL entrypoint sources this script only for a fresh volume.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c 'CREATE EXTENSION IF NOT EXISTS timescaledb'
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -f /db/init/001_roles_and_extension.sql

for role in ktm_migrator ktm_app; do
    password_file="/run/secrets/${role}_password"
    [ -r "$password_file" ] && [ -s "$password_file" ] || {
        printf 'Missing or empty secret for %s\n' "$role" >&2
        exit 1
    }
done

KTM_LOCAL_MIGRATOR_PASSWORD=$(cat /run/secrets/ktm_migrator_password)
KTM_LOCAL_APP_PASSWORD=$(cat /run/secrets/ktm_app_password)
export KTM_LOCAL_MIGRATOR_PASSWORD KTM_LOCAL_APP_PASSWORD

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv migrator_password KTM_LOCAL_MIGRATOR_PASSWORD
\getenv app_password KTM_LOCAL_APP_PASSWORD
ALTER ROLE ktm_migrator PASSWORD :'migrator_password';
ALTER ROLE ktm_app PASSWORD :'app_password';
SQL

unset KTM_LOCAL_MIGRATOR_PASSWORD KTM_LOCAL_APP_PASSWORD
