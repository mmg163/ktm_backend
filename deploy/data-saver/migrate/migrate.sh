#!/bin/sh
# Накат db/migrations/V*.sql под ролью ktm_migrator (ADR-0004, раздел 5; openspec add-data-saver-migrate-job).
# POSIX sh (busybox, образ postgres:*-alpine). Метакоманды psql - только в обёртке, в V-файлах их нет.
#
# Конфигурация (окружение):
#   DataSaver__Postgres__Host, __Database   обязательны
#   DataSaver__Postgres__Port               по умолчанию 5432
#   DataSaver__Postgres__SslMode            Disable|Prefer|Require (по умолчанию)|VerifyCA|VerifyFull
#   MIGRATE_PASSWORD_FILE                   файл пароля (по умолчанию /run/secrets/ktm_migrator_password)
#   MIGRATE_DIR                             каталог миграций (по умолчанию /db/migrations)
#   MIGRATE_LOCK_TIMEOUT_SECONDS            ожидание блокировки (по умолчанию 120)
#   MIGRATE_USERNAME                        только для тестов; по умолчанию ktm_migrator
#   DataSaver__Postgres__Username / PasswordFile (роль ktm_app) НЕ используются.
#
# Коды выхода: 0 успех; 1 ошибка миграции, состава файлов или подключения; 2 ошибка конфигурации/секрета;
#              3 таймаут блокировки или потеря сеанса блокировки.
#
# Пароль: только из файла (Docker secret), в .pgpass (tmpfs, 0600). В argv, окружение и вывод не попадает;
# set -x не использовать.

set -eu
LC_ALL=C
export LC_ALL

log() { printf 'migrate: %s\n' "$*"; }
die() {
    _code=$1
    shift
    printf 'migrate: ERROR: %s\n' "$*" >&2
    exit "$_code"
}

# ---------- 1. Конфигурация ----------

require_env() {
    eval "_v=\${$1:-}"
    [ -n "$_v" ] || die 2 "required environment variable $1 is not set"
}

require_env DataSaver__Postgres__Host
require_env DataSaver__Postgres__Database
PGHOST=$DataSaver__Postgres__Host
PGDATABASE=$DataSaver__Postgres__Database
PGPORT=${DataSaver__Postgres__Port:-5432}
case $PGPORT in
    '' | *[!0-9]*) die 2 "DataSaver__Postgres__Port must be a number" ;;
esac

_ssl=$(printf '%s' "${DataSaver__Postgres__SslMode:-Require}" | tr 'A-Z' 'a-z')
case $_ssl in
    disable) PGSSLMODE=disable ;;
    prefer) PGSSLMODE=prefer ;;
    require) PGSSLMODE=require ;;
    verifyca) PGSSLMODE=verify-ca ;;
    verifyfull) PGSSLMODE=verify-full ;;
    *) die 2 "DataSaver__Postgres__SslMode has unknown value (expected Disable, Prefer, Require, VerifyCA, VerifyFull)" ;;
esac

MIGRATE_USER=${MIGRATE_USERNAME:-ktm_migrator}
PASSWORD_FILE=${MIGRATE_PASSWORD_FILE:-/run/secrets/ktm_migrator_password}
MIGRATIONS_DIR=${MIGRATE_DIR:-/db/migrations}
LOCK_TIMEOUT=${MIGRATE_LOCK_TIMEOUT_SECONDS:-120}
case $LOCK_TIMEOUT in
    '' | *[!0-9]* | 0) die 2 "MIGRATE_LOCK_TIMEOUT_SECONDS must be a positive integer" ;;
esac

unset PGPASSWORD PGPASSFILE PGSERVICE PGSERVICEFILE
export PGHOST PGPORT PGDATABASE PGSSLMODE
export PGUSER="$MIGRATE_USER"
PGCONNECT_TIMEOUT=10
export PGCONNECT_TIMEOUT

# ---------- 2. Секрет: файл -> .pgpass в tmpfs ----------

[ -f "$PASSWORD_FILE" ] && [ -r "$PASSWORD_FILE" ] || die 2 "password file is missing or not readable: $PASSWORD_FILE"


umask 077
TMP_DIR=$(mktemp -d /tmp/migrate.XXXXXX) || die 2 "cannot create temporary directory in /tmp"

HOLDER_FD_OPEN=0
cleanup() {
    _rc=$?
    trap - EXIT INT TERM
    if [ "$HOLDER_FD_OPEN" = 1 ]; then
        exec 3>&-
    fi
    if [ -f "$TMP_DIR/holder.pid" ] && [ ! -f "$TMP_DIR/holder.rc" ]; then
        # EOF на входе завершает держателя штатно; принудительно - только если он завис (например, ждёт блокировку).
        _i=0
        while [ ! -f "$TMP_DIR/holder.rc" ] && [ "$_i" -lt 20 ]; do
            sleep 0.1
            _i=$((_i + 1))
        done
        [ -f "$TMP_DIR/holder.rc" ] || kill "$(cat "$TMP_DIR/holder.pid")" 2>/dev/null || true
    fi
    rm -rf "$TMP_DIR"
    exit "$_rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap '' PIPE

_pw=
IFS= read -r _pw <"$PASSWORD_FILE" || [ -n "$_pw" ] || true
[ -n "$_pw" ] || die 2 "password file is empty: $PASSWORD_FILE"
# Формат .pgpass: host:port:db:user:password; '\' и ':' экранируются обратной косой чертой.
_esc_pw=$(printf '%s' "$_pw" | sed -e 's/\\/\\\\/g' -e 's/:/\\:/g')
_esc_user=$(printf '%s' "$MIGRATE_USER" | sed -e 's/\\/\\\\/g' -e 's/:/\\:/g')
PGPASSFILE=$TMP_DIR/pgpass
printf '*:*:*:%s:%s\n' "$_esc_user" "$_esc_pw" >"$PGPASSFILE"
_esc_pw=
_pw=
export PGPASSFILE

# ---------- 3. Состав и порядок файлов ----------

[ -d "$MIGRATIONS_DIR" ] || die 1 "migrations directory not found: $MIGRATIONS_DIR"
find "$MIGRATIONS_DIR" -maxdepth 1 -name 'V*.sql' 2>/dev/null | sed 's|.*/||' | sort >"$TMP_DIR/files"
[ -s "$TMP_DIR/files" ] || die 1 "no files matching V*.sql in $MIGRATIONS_DIR"

_prev=-1
while IFS= read -r _name; do
    printf '%s\n' "$_name" | grep -Eq '^V[0-9]{1,9}__[A-Za-z0-9_.-]+\.sql$' ||
        die 1 "file name does not match V<number>__<name>.sql: $_name"
    [ -f "$MIGRATIONS_DIR/$_name" ] || die 1 "not a regular file: $_name"
    _num=$(printf '%s' "$_name" | sed -E 's/^V0*([0-9]+)__.*/\1/')
    [ "$_num" -ne "$_prev" ] || die 1 "duplicate migration number $_num (file $_name)"
    # Порядок задаёт имя; если числовой порядок с ним расходится (V2 и V10), результат неоднозначен.
    [ "$_num" -gt "$_prev" ] || die 1 "name order differs from numeric order at $_name: use zero-padded numbers"
    _prev=$_num
done <"$TMP_DIR/files"

# ---------- 4. Блокировка: фоновый сеанс-держатель ----------

# Ключ один на базу; тесты используют то же выражение.
LOCK_KEY="hashtextextended('ktm:schema-migration', 0)"
HOLDER_OUT=$TMP_DIR/holder.out
: >"$HOLDER_OUT"

holder_alive() { [ ! -f "$TMP_DIR/holder.rc" ]; }

send() { printf '%s\n' "$1" >&3 2>/dev/null; }

# wait_line <regex> <seconds>: 0 - строка появилась, 1 - держатель завершился, 2 - время вышло.
wait_line() {
    _deadline=$(($(date +%s) + $2))
    while :; do
        if grep -Eq "$1" "$HOLDER_OUT"; then return 0; fi
        holder_alive || return 1
        [ "$(date +%s)" -lt "$_deadline" ] || return 2
        sleep 0.1
    done
}

show_holder_output() { [ ! -s "$HOLDER_OUT" ] || cat "$HOLDER_OUT" >&2; }

mkfifo "$TMP_DIR/holder.in"
(
    PGAPPNAME=ktm-migrate-lock psql -X -q -A -t -v ON_ERROR_STOP=1 <"$TMP_DIR/holder.in" >"$HOLDER_OUT" 2>&1 &
    _hp=$!
    printf '%s\n' "$_hp" >"$TMP_DIR/holder.pid"
    _hrc=0
    wait "$_hp" || _hrc=$?
    printf '%s\n' "$_hrc" >"$TMP_DIR/holder.rc"
) &
exec 3>"$TMP_DIR/holder.in"
HOLDER_FD_OPEN=1

# psql подключается при старте: ошибка подключения/SSL/пароля - это завершение держателя.
send '\echo KTM_SESSION_OK' || true
_rc=0
wait_line '^KTM_SESSION_OK$' 30 || _rc=$?
if [ "$_rc" -ne 0 ]; then
    show_holder_output
    die 1 "cannot open session to $PGHOST:$PGPORT/$PGDATABASE as $MIGRATE_USER (sslmode=$PGSSLMODE)"
fi

send "SELECT 'KTM_TRY_' || pg_try_advisory_lock($LOCK_KEY)::text;" || die 3 "lock session lost"
_rc=0
wait_line '^KTM_TRY_(true|false)$' 30 || _rc=$?
[ "$_rc" -eq 0 ] || { show_holder_output; die 3 "lock session lost"; }
if grep -q '^KTM_TRY_false$' "$HOLDER_OUT"; then
    log "migration lock is held by another session, waiting up to ${LOCK_TIMEOUT}s"
    # lock_timeout ограничивает ожидание на стороне сервера: сеанс не останется ждать после выхода скрипта.
    send "SET lock_timeout = $((LOCK_TIMEOUT * 1000)); SELECT pg_advisory_lock($LOCK_KEY); \\echo KTM_LOCKED" ||
        die 3 "lock session lost"
    _rc=0
    wait_line '^KTM_LOCKED$' $((LOCK_TIMEOUT + 15)) || _rc=$?
    if [ "$_rc" -ne 0 ]; then
        show_holder_output
        die 3 "timed out waiting for migration lock (${LOCK_TIMEOUT}s)"
    fi
fi
log "migration lock acquired"

# ---------- 5. Прогон ----------

_ping=0
_done=0
while IFS= read -r _name; do
    # Живость сеанса проверяется круговым запросом: процесс psql может быть жив при разорванном соединении.
    _ping=$((_ping + 1))
    _rc=0
    send "SELECT 'KTM_ALIVE_$_ping';" || _rc=1
    if [ "$_rc" -eq 0 ]; then wait_line "^KTM_ALIVE_$_ping\$" 15 || _rc=$?; fi
    if [ "$_rc" -ne 0 ]; then
        show_holder_output
        die 3 "lock session lost before $_name; file not applied"
    fi

    log "apply $_name"
    _rc=0
    PGAPPNAME=ktm-migrate psql -X -v ON_ERROR_STOP=1 -f "$MIGRATIONS_DIR/$_name" </dev/null || _rc=$?
    [ "$_rc" -eq 0 ] || die 1 "$_name failed (psql exit code $_rc); remaining files not applied"
    _done=$((_done + 1))
done <"$TMP_DIR/files"

log "done $_done files"
