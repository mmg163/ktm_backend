#!/bin/sh
# The official PostgreSQL entrypoint sources this script only for a fresh volume.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c 'CREATE EXTENSION IF NOT EXISTS timescaledb'
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -f /db/init/001_roles_and_extension.sql

for role in ktm_migrator ktm_app ktm_api ktm_binding ktm_interpretation; do
    password_file="/run/secrets/${role}_password"
    [ -r "$password_file" ] && [ -s "$password_file" ] || {
        printf 'Missing or empty secret for %s\n' "$role" >&2
        exit 1
    }
done

KTM_LOCAL_MIGRATOR_PASSWORD=$(cat /run/secrets/ktm_migrator_password)
KTM_LOCAL_APP_PASSWORD=$(cat /run/secrets/ktm_app_password)
KTM_LOCAL_API_PASSWORD=$(cat /run/secrets/ktm_api_password)
KTM_LOCAL_BINDING_PASSWORD=$(cat /run/secrets/ktm_binding_password)
KTM_LOCAL_INTERPRETATION_PASSWORD=$(cat /run/secrets/ktm_interpretation_password)
export KTM_LOCAL_MIGRATOR_PASSWORD KTM_LOCAL_APP_PASSWORD KTM_LOCAL_API_PASSWORD
export KTM_LOCAL_BINDING_PASSWORD KTM_LOCAL_INTERPRETATION_PASSWORD

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'SQL'
\getenv migrator_password KTM_LOCAL_MIGRATOR_PASSWORD
\getenv app_password KTM_LOCAL_APP_PASSWORD
\getenv api_password KTM_LOCAL_API_PASSWORD
\getenv binding_password KTM_LOCAL_BINDING_PASSWORD
\getenv interpretation_password KTM_LOCAL_INTERPRETATION_PASSWORD
CREATE ROLE ktm_api LOGIN;
CREATE ROLE ktm_binding LOGIN;
CREATE ROLE ktm_interpretation LOGIN;
ALTER ROLE ktm_migrator PASSWORD :'migrator_password';
ALTER ROLE ktm_app PASSWORD :'app_password';
ALTER ROLE ktm_api PASSWORD :'api_password';
ALTER ROLE ktm_binding PASSWORD :'binding_password';
ALTER ROLE ktm_interpretation PASSWORD :'interpretation_password';
GRANT CONNECT ON DATABASE ktm_revisor TO ktm_api, ktm_binding, ktm_interpretation;
GRANT USAGE ON SCHEMA ktm TO ktm_api, ktm_binding, ktm_interpretation;
SQL

unset KTM_LOCAL_MIGRATOR_PASSWORD KTM_LOCAL_APP_PASSWORD KTM_LOCAL_API_PASSWORD
unset KTM_LOCAL_BINDING_PASSWORD KTM_LOCAL_INTERPRETATION_PASSWORD
