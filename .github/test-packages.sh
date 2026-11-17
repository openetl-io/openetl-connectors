#!/usr/bin/env bash
# Packages connectors for this machine's platform, as a release does, and
# checks each role of what it wrote with openetl connector test, against the
# services the integration tests use. The destinations' checks that write need
# a way to read them back, which the databases have here, through their
# clients or their containers'; the integration tests run those checks on
# every destination.
#
#   test-packages.sh                 every package in this checkout
#   test-packages.sh postgres ...    those packages in this checkout
#   test-packages.sh postgres=TARGET ...
#                                    those packages at TARGET, packaging
#                                    nothing, such as a released version:
#                                    oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.1.0-alpha
#
# A package's checks are the function checks_NAME below, and need only that
# package's services. OPENETL is the openetl CLI. KAFKA_CONTAINER,
# ORACLE_CONTAINER, MONGODB_CONTAINER and SQLSERVER_CONTAINER are the
# containers whose clients the checks run; MySQL's and MariaDB's are mysql and
# mariadb.
set -uo pipefail

openetl=${OPENETL:?the openetl CLI}
export KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
export ORACLE_CONTAINER=${ORACLE_CONTAINER:-oracle}
export PG=${OPENETL_TEST_POSTGRES_DSN:-postgres://openetl:openetl@127.0.0.1:5432/openetl}
export PGTZ=UTC
kafka=${OPENETL_TEST_KAFKA_BROKERS:-127.0.0.1:9092}
kafka_sasl=${OPENETL_TEST_KAFKA_SASL_BROKERS:-127.0.0.1:9094}
kinesis=${OPENETL_TEST_KINESIS_ENDPOINT:-http://127.0.0.1:4568}
s3=${OPENETL_TEST_S3_ENDPOINT:-http://127.0.0.1:8333}
iceberg=${OPENETL_TEST_ICEBERG_URI:-http://127.0.0.1:8181}
export AWS_ACCESS_KEY_ID=openetl AWS_SECRET_ACCESS_KEY=openetl AWS_DEFAULT_REGION=us-east-1
run=pkg$(date +%s)
work=$(mktemp -d)
export RUN=$run WORK=$work KINESIS=$kinesis
dist=$work/dist
cleanup() {
  psql "$PG" -qc "DROP SCHEMA IF EXISTS $run CASCADE; DROP PUBLICATION IF EXISTS $run" >/dev/null 2>&1
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$run\\..*" >/dev/null 2>&1
  for s in a b; do aws --endpoint-url "$kinesis" kinesis delete-stream --stream-name "$run-$s" >/dev/null 2>&1; done
  for c in mysql mariadb; do echo "DROP DATABASE IF EXISTS $run" | "$work/my-sql" $c $c >/dev/null 2>&1; done
  docker exec -i "$ORACLE_CONTAINER" sqlplus -s -L system/openetl@//localhost:1521/FREEPDB1 >/dev/null 2>&1 <<SQL
BEGIN
  FOR t IN (SELECT table_name FROM all_tables WHERE owner = 'OPENETL' AND table_name LIKE UPPER('$run') || '\_%' ESCAPE '\') LOOP
    EXECUTE IMMEDIATE 'DROP TABLE OPENETL."' || t.table_name || '" PURGE';
  END LOOP;
  EXECUTE IMMEDIATE 'DROP USER openetl_dest CASCADE';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/
SQL
  rm -rf "$work"
}
trap cleanup EXIT

# The packages in this checkout, NAME=DIR each.
here=()
for f in */openetl-connector.yaml; do
  here+=("$(sed -n 's/^name: *//p' "$f")=${f%%/*}")
done
# package_dir NAME is the directory of package NAME in this checkout.
package_dir() {
  local p
  for p in "${here[@]}"; do [ "${p%%=*}" != "$1" ] || { echo "${p#*=}"; return; }; done
}

# target NAME is what package NAME's checks run against.
target() { local v=target_$1; echo "${!v:-}"; }
(($#)) || set -- "${here[@]%%=*}"
names=() build=()
for a in "$@"; do
  name=${a%%=*}
  [[ "$name" =~ ^[a-z0-9]+$ ]] || { echo "$a isn't NAME or NAME=TARGET" >&2; exit 2; }
  names+=("$name")
  if [[ "$a" == *=* ]]; then
    [ -n "${a#*=}" ] || { echo "$a has no TARGET" >&2; exit 2; }
    printf -v "target_$name" '%s' "${a#*=}"
  else
    d=$(package_dir "$name")
    [ -n "$d" ] || { echo "no package $name here" >&2; exit 2; }
    build+=("$d")
    printf -v "target_$name" '%s' "$dist/$name.connector.yaml"
  fi
done
if ((${#build[@]})); then
  "$openetl" connector package --version 0.0.0-ci --platforms "$(go env GOOS)/$(go env GOARCH)" --out "$dist" \
    "${build[@]}" || exit 1
fi

failed=()
# check NAME PACKAGE FLAGS... runs the kit on PACKAGE's target with FLAGS.
check() {
  local name=$1 at
  at=$(target "$2")
  shift 2
  [ -n "$at" ] || return 0
  echo "::group::$name"
  if ! "$openetl" connector test -q "$@" "$at"; then
    failed+=("$name")
  fi
  echo "::endgroup::"
}
# stops NAME FLAGS... prints FLAGS, the stops a redo asks for that package
# NAME honours, where it's packaged from this checkout. A release from before
# it honoured them is checked without.
stops() {
  [[ "$(target "$1")" == "$dist"/* ]] || return 0
  shift
  echo "$@"
}
# config NAME JSON writes the config $work/NAME.json.
config() { printf '%s' "$2" >"$work/$1.json"; }
# script NAME writes stdin as an executable script NAME.
script() { { echo '#!/bin/sh'; cat; } >"$work/$1" && chmod +x "$work/$1"; }

# Postgres: a table read in full, one incrementally and one with cdc.
checks_postgres() {
  psql "$PG" -v ON_ERROR_STOP=1 -q <<SQL || exit 1
CREATE SCHEMA $run;
CREATE TABLE $run.users (id bigint PRIMARY KEY, name text, joined date);
INSERT INTO $run.users SELECT i, 'u' || i, date '2020-01-01' + i FROM generate_series(1, 120) i;
CREATE TABLE $run.events (id bigint PRIMARY KEY, at timestamptz NOT NULL, body jsonb);
INSERT INTO $run.events SELECT i, timestamptz '2026-01-01 00:00Z' + i * interval '1 minute', jsonb_build_object('n', i)
  FROM generate_series(1, 90) i;
CREATE TABLE $run.orders (id bigint PRIMARY KEY, total numeric(10, 2), placed timestamptz);
INSERT INTO $run.orders SELECT i, i * 1.25, timestamptz '2026-03-01 00:00Z' + i * interval '1 hour' FROM generate_series(1, 70) i;
SQL
  pg_source() { printf '{"dsn": "%s", "schema": "%s", "chunkRows": 25, "snapshotChunkRows": 20, "publication": "%s"}' "$1" "$run" "$run"; }
  bad_pg=${PG/:openetl@/:not-the-password@}
  closed_pg="postgres://openetl:openetl@127.0.0.1:1/openetl?connect_timeout=2"
  script pg-add <<'SH'
case $OPENETL_STREAM in
events)
  psql "$PG" -qc "INSERT INTO $RUN.events SELECT i, now(), '{}'
    FROM generate_series((SELECT max(id) + 1 FROM $RUN.events), (SELECT max(id) + 10 FROM $RUN.events)) i" >/dev/null && echo 10 ;;
orders)
  psql "$PG" -qc "INSERT INTO $RUN.orders SELECT i, i * 1.25, now()
    FROM generate_series((SELECT max(id) + 1 FROM $RUN.orders), (SELECT max(id) + 5 FROM $RUN.orders)) i" >/dev/null && echo 5 ;;
*) echo 0 ;;
esac
SH
  script pg-read <<'SH'
psql "$PG" -qAt -v ON_ERROR_STOP=1 -v ns="$OPENETL_NAMESPACE" -v st="$OPENETL_STREAM" <<'SQL'
SELECT format('SELECT row_to_json(t) FROM %I.%I t', :'ns', :'st') WHERE to_regclass(format('%I.%I', :'ns', :'st')) IS NOT NULL \gexec
SQL
SH
  script pg-drop <<'SH'
psql "$PG" -qc "DROP SCHEMA IF EXISTS \"$OPENETL_NAMESPACE\" CASCADE" >/dev/null
SH
  config pg-source "$(pg_source "$PG")"
  config pg-source-bad "$(pg_source "$bad_pg")"
  config pg-source-closed "$(pg_source "$closed_pg")"
  check source-postgres postgres --role source \
    --config "$work/pg-source.json" \
    --bad-credentials "$work/pg-source-bad.json" \
    --unreachable "$work/pg-source-closed.json" \
    --stream users:full_refresh --stream events:incremental:at --stream orders:cdc \
    --add-records-command "$work/pg-add" \
    $(stops postgres --stops-at-completion)
  config pg-dest "{\"dsn\": \"$PG\"}"
  config pg-dest-bad "{\"dsn\": \"$bad_pg\"}"
  config pg-dest-closed "{\"dsn\": \"$closed_pg\"}"
  check destination-postgres postgres --role destination \
    --config "$work/pg-dest.json" \
    --bad-credentials "$work/pg-dest-bad.json" \
    --unreachable "$work/pg-dest-closed.json" \
    --read-command "$work/pg-read" --drop-command "$work/pg-drop"
}

# MySQL and MariaDB: the source reads a table in full, one by a cursor and
# one by CDC; the destination writes. Each against both servers.
checks_mysql() {
  script my-sql <<'SH'
exec docker exec -i -e MYSQL_PWD=openetl "$1" "$2" -h127.0.0.1 -uopenetl --default-character-set=utf8mb4 -N -B -r
SH
  script my-add <<'SH'
case $OPENETL_STREAM in
orders) at='NOW(6)' ;;
events) at="TIMESTAMP '2030-01-01 00:00:00'" ;;
*) echo 0; exit ;;
esac
"$WORK/my-sql" "$1" "$2" <<SQL >/dev/null && echo 5
INSERT INTO $RUN.$OPENETL_STREAM SELECT m + i, i, $at
  FROM (SELECT MAX(id) AS m FROM $RUN.$OPENETL_STREAM) x, (SELECT 1 i UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) n;
SQL
SH
  # Each row as a JSON object: booleans as true and false, binary as \x hex,
  # FLOAT in full, and text in one collation, as MariaDB's JSON columns have
  # their own.
  script my-read <<'SH'
{
  printf "SET @ns = '%s', @st = '%s';\n" "$OPENETL_NAMESPACE" "$OPENETL_STREAM"
  cat <<'SQL'
SET SESSION group_concat_max_len = 1048576;
SET @q = COALESCE((
  SELECT CONCAT('SELECT JSON_OBJECT(', GROUP_CONCAT(CONCAT(QUOTE(COLUMN_NAME), ', ', CASE
    WHEN COLUMN_TYPE = 'tinyint(1)' THEN CONCAT('CASE WHEN `', COLUMN_NAME, '` IS NULL THEN NULL WHEN `', COLUMN_NAME, '` THEN ''true'' ELSE ''false'' END')
    WHEN DATA_TYPE IN ('binary', 'varbinary', 'tinyblob', 'blob', 'mediumblob', 'longblob') THEN CONCAT('CONCAT(''\\\\x'', HEX(`', COLUMN_NAME, '`))')
    WHEN DATA_TYPE = 'float' THEN CONCAT('CAST(`', COLUMN_NAME, '` AS DOUBLE)')
    WHEN COLLATION_NAME IS NOT NULL THEN CONCAT('CONVERT(`', COLUMN_NAME, '` USING utf8mb4)')
    ELSE CONCAT('`', COLUMN_NAME, '`') END) ORDER BY ORDINAL_POSITION), ') FROM `', @ns, '`.`', @st, '`')
  FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = @ns AND TABLE_NAME = @st), 'SELECT 1 FROM DUAL WHERE FALSE');
PREPARE q FROM @q;
EXECUTE q;
SQL
} | "$WORK/my-sql" "$1" "$2"
SH
  script my-drop <<'SH'
echo "DROP DATABASE IF EXISTS \`$OPENETL_NAMESPACE\`" | "$WORK/my-sql" "$1" "$2"
SH
  local srv container client dsn bad closed
  for srv in mysql:mysql:"${OPENETL_TEST_MYSQL_DSN:-openetl:openetl@tcp(127.0.0.1:3306)/openetl}" \
    mariadb:mariadb:"${OPENETL_TEST_MARIADB_DSN:-openetl:openetl@tcp(127.0.0.1:3307)/openetl}"; do
    IFS=: read -r container client dsn <<<"$srv"
    bad=${dsn/:openetl@/:not-the-password@}
    closed="openetl:openetl@tcp(127.0.0.1:1)/openetl?timeout=2s"
    "$work/my-sql" "$container" "$client" <<SQL || exit 1
CREATE DATABASE $run;
CREATE TABLE $run.users (id bigint PRIMARY KEY, name varchar(20), joined date);
INSERT INTO $run.users WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 120)
  SELECT i, CONCAT('u', i), DATE '2020-01-01' + INTERVAL i DAY FROM s;
CREATE TABLE $run.events (id bigint PRIMARY KEY, kind int, at datetime(6));
INSERT INTO $run.events WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 50)
  SELECT i, i % 3, TIMESTAMP '2026-03-01 00:00:00' + INTERVAL i MINUTE FROM s;
CREATE TABLE $run.orders (id bigint PRIMARY KEY, total decimal(10, 2), placed timestamp(6));
INSERT INTO $run.orders WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM s WHERE i < 70)
  SELECT i, i * 1.25, TIMESTAMP '2026-03-01 00:00:00' + INTERVAL i HOUR FROM s;
SQL
    my_source() { printf '{"dsn": "%s", "database": "%s", "chunkRows": 25, "snapshotChunkRows": 20}' "$1" "$run"; }
    config "$container-source" "$(my_source "$dsn")"
    config "$container-source-bad" "$(my_source "$bad")"
    config "$container-source-closed" "$(my_source "$closed")"
    check "source-mysql ($container)" mysql --role source \
      --config "$work/$container-source.json" \
      --bad-credentials "$work/$container-source-bad.json" \
      --unreachable "$work/$container-source-closed.json" \
      --stream users:full_refresh --stream events:incremental:at --stream orders:cdc \
      --add-records-command "$work/my-add $container $client" \
      --stops-at-until
    config "$container-dest" "{\"dsn\": \"$dsn\"}"
    config "$container-dest-bad" "{\"dsn\": \"$bad\"}"
    config "$container-dest-closed" "{\"dsn\": \"$closed\"}"
    check "destination-mysql ($container)" mysql --role destination \
      --config "$work/$container-dest.json" \
      --bad-credentials "$work/$container-dest-bad.json" \
      --unreachable "$work/$container-dest-closed.json" \
      --read-command "$work/my-read $container $client" --drop-command "$work/my-drop $container $client" \
      --lossy-type "float32=NaN and infinities are stored as null" --lossy-type "float64=NaN and infinities are stored as null"
  done
}

# SQL Server: a table read in full and one read with cdc, by change tracking.
# The scripts run sqlcmd as sa in the container SQLSERVER_CONTAINER.
checks_sqlserver() {
  export SQLSERVER_CONTAINER=${SQLSERVER_CONTAINER:-sqlserver}
  local ms=${OPENETL_TEST_SQLSERVER_DSN:-sqlserver://openetl:OpenETL-test-1@127.0.0.1:1433?database=openetl}
  script ms <<'SH'
exec docker exec -i "$SQLSERVER_CONTAINER" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P OpenETL-test-1 -C -d openetl -b "$@"
SH
  "$work/ms" >/dev/null <<SQL || exit 1
CREATE SCHEMA $run;
GO
GRANT SELECT, VIEW CHANGE TRACKING ON SCHEMA::$run TO openetl;
CREATE TABLE $run.users (id bigint PRIMARY KEY, name nvarchar(20), joined date);
INSERT INTO $run.users SELECT n, CONCAT('u', n), DATEADD(day, n, '2020-01-01') FROM (SELECT TOP (120) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x;
CREATE TABLE $run.orders (id bigint PRIMARY KEY, total decimal(10, 2), placed datetimeoffset);
INSERT INTO $run.orders SELECT n, n * 1.25, DATEADD(hour, n, CONVERT(datetimeoffset, '2026-03-01 00:00 +00:00')) FROM (SELECT TOP (70) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) x;
ALTER TABLE $run.orders ENABLE CHANGE_TRACKING;
SQL
  ms_source() { printf '{"dsn": "%s", "schema": "%s", "chunkRows": 25, "snapshotChunkRows": 20}' "$1" "$run"; }
  local bad_ms=${ms/:OpenETL-test-1@/:not-the-password@}
  local closed_ms="sqlserver://openetl:x@127.0.0.1:1?database=openetl&dial+timeout=2"
  script ms-add <<'SH'
case $OPENETL_STREAM in
orders)
  "$WORK/ms" -Q "SET NOCOUNT ON; INSERT INTO $RUN.orders SELECT m + n, (m + n) * 1.25, SYSDATETIMEOFFSET()
    FROM (SELECT max(id) AS m FROM $RUN.orders) x CROSS JOIN (SELECT TOP (5) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_objects) y" >/dev/null && echo 5 ;;
*) echo 0 ;;
esac
SH
  script ms-read <<'SH'
"$WORK/ms" -y 0 -v ns="$OPENETL_NAMESPACE" st="$OPENETL_STREAM" <<'SQL'
SET NOCOUNT ON;
DECLARE @t nvarchar(600) = QUOTENAME(N'$(ns)') + N'.' + QUOTENAME(N'$(st)');
DECLARE @q nvarchar(max) = N'SELECT (SELECT t.* FOR JSON PATH, WITHOUT_ARRAY_WRAPPER, INCLUDE_NULL_VALUES) FROM ' + @t + N' AS t';
IF OBJECT_ID(@t, N'U') IS NOT NULL EXEC (@q);
SQL
SH
  script ms-drop <<'SH'
"$WORK/ms" -v ns="$OPENETL_NAMESPACE" >/dev/null <<'SQL'
DECLARE @s sysname = N'$(ns)', @q nvarchar(max) = N'';
SELECT @q += N'DROP TABLE ' + QUOTENAME(@s) + N'.' + QUOTENAME(name) + N'; ' FROM sys.tables WHERE schema_id = SCHEMA_ID(@s);
IF SCHEMA_ID(@s) IS NOT NULL SET @q += N'DROP SCHEMA ' + QUOTENAME(@s);
EXEC (@q);
SQL
SH
  config ms-source "$(ms_source "$ms")"
  config ms-source-bad "$(ms_source "$bad_ms")"
  config ms-source-closed "$(ms_source "$closed_ms")"
  check source-sqlserver sqlserver --role source \
    --config "$work/ms-source.json" \
    --bad-credentials "$work/ms-source-bad.json" \
    --unreachable "$work/ms-source-closed.json" \
    --stream users:full_refresh --stream orders:cdc --add-records-command "$work/ms-add" \
    --stops-at-until
  config ms-dest "{\"dsn\": \"$ms\"}"
  config ms-dest-bad "{\"dsn\": \"$bad_ms\"}"
  config ms-dest-closed "{\"dsn\": \"$closed_ms\"}"
  check destination-sqlserver sqlserver --role destination \
    --config "$work/ms-dest.json" \
    --bad-credentials "$work/ms-dest-bad.json" \
    --unreachable "$work/ms-dest-closed.json" \
    --read-command "$work/ms-read" --drop-command "$work/ms-drop" \
    --lossy-type 'float32=NaN and infinities land as NULL: real holds neither' \
    --lossy-type 'float64=NaN and infinities land as NULL: float holds neither'
  OPENETL_NAMESPACE=$run "$work/ms-drop"
}

# Oracle: a table read in full, one by a cursor and one with cdc through
# LogMiner, as the user openetl, which the services' setup grants LogMiner;
# and tables written to schemas of their own, as openetl_dest.
checks_oracle() {
  # ora_sql USER/PASSWORD runs stdin with sqlplus in the container's FREEPDB1.
  ora_sql() { docker exec -i "$ORACLE_CONTAINER" sqlplus -s -L "$1@//localhost:1521/FREEPDB1"; }
  up=$(tr a-z A-Z <<<"$run")
  ora_sql openetl/openetl >/dev/null <<SQL || exit 1
WHENEVER SQLERROR EXIT FAILURE
CREATE TABLE ${up}_USERS (id NUMBER(19) PRIMARY KEY, name VARCHAR2(50), joined DATE);
INSERT INTO ${up}_USERS SELECT level, 'u' || level, DATE '2020-01-01' + level FROM dual CONNECT BY level <= 120;
CREATE TABLE ${up}_EVENTS (id NUMBER(19) PRIMARY KEY, at TIMESTAMP(6) NOT NULL, body VARCHAR2(100));
INSERT INTO ${up}_EVENTS SELECT level, TIMESTAMP '2026-01-01 00:00:00' + NUMTODSINTERVAL(level, 'MINUTE'), 'e' || level FROM dual CONNECT BY level <= 90;
CREATE TABLE ${up}_ORDERS (id NUMBER(19) PRIMARY KEY, total NUMBER(10, 2), placed TIMESTAMP WITH TIME ZONE);
ALTER TABLE ${up}_ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
INSERT INTO ${up}_ORDERS SELECT level, level * 1.25, TIMESTAMP '2026-03-01 00:00:00 UTC' + NUMTODSINTERVAL(level, 'HOUR') FROM dual CONNECT BY level <= 70;
COMMIT;
SQL
  ora_sql system/openetl >/dev/null <<'SQL' || exit 1
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  EXECUTE IMMEDIATE 'DROP USER openetl_dest CASCADE';
EXCEPTION WHEN OTHERS THEN
  IF SQLCODE != -1918 THEN RAISE; END IF;
END;
/
CREATE USER openetl_dest IDENTIFIED BY "openetl" QUOTA UNLIMITED ON USERS;
GRANT CREATE SESSION, CREATE TABLE, CREATE USER, CREATE ANY TABLE, ALTER ANY TABLE, SELECT ANY TABLE,
  INSERT ANY TABLE, UPDATE ANY TABLE, DELETE ANY TABLE, CREATE ANY INDEX TO openetl_dest;
SQL
  ora_dsn() { printf 'oracle://%s@127.0.0.1:%s/FREEPDB1%s' "$@"; }
  ora_source() { printf '{"dsn": "%s", "tables": ["%s_USERS", "%s_EVENTS", "%s_ORDERS"], "chunkRows": 25, "snapshotChunkRows": 20}' "$1" "$up" "$up" "$up"; }
  # A user that doesn't exist, which no failed sign-in can lock.
  bad_ora=$(ora_dsn openetl_nobody:not-the-password 1521 '')
  closed_ora=$(ora_dsn openetl:openetl 1 '?CONNECTION%20TIMEOUT=2')
  script ora-add <<'SH'
case $OPENETL_STREAM in
*_EVENTS)
  docker exec -i "$ORACLE_CONTAINER" sqlplus -s -L openetl/openetl@//localhost:1521/FREEPDB1 >/dev/null <<SQL && echo 10 ;;
WHENEVER SQLERROR EXIT FAILURE
INSERT INTO $OPENETL_STREAM SELECT m + level, TIMESTAMP '2026-02-01 00:00:00' + NUMTODSINTERVAL(m + level, 'MINUTE'), 'new'
  FROM (SELECT max(id) m FROM $OPENETL_STREAM) CONNECT BY level <= 10;
COMMIT;
SQL
*_ORDERS)
  docker exec -i "$ORACLE_CONTAINER" sqlplus -s -L openetl/openetl@//localhost:1521/FREEPDB1 >/dev/null <<SQL && echo 5 ;;
WHENEVER SQLERROR EXIT FAILURE
INSERT INTO $OPENETL_STREAM SELECT m + level, (m + level) * 1.25, SYSTIMESTAMP
  FROM (SELECT max(id) m FROM $OPENETL_STREAM) CONNECT BY level <= 5;
COMMIT;
SQL
*) echo 0 ;;
esac
SH
  config ora-source "$(ora_source "$(ora_dsn openetl:openetl 1521 '')")"
  config ora-source-bad "$(ora_source "$bad_ora")"
  config ora-source-closed "$(ora_source "$closed_ora")"
  check source-oracle oracle --role source \
    --config "$work/ora-source.json" \
    --bad-credentials "$work/ora-source-bad.json" \
    --unreachable "$work/ora-source-closed.json" \
    --stream "${up}_USERS:full_refresh" --stream "${up}_EVENTS:incremental:AT" --stream "${up}_ORDERS:cdc" \
    --add-records-command "$work/ora-add" --stops-at-until
  # A table's rows as JSON lines: a time of day as HH:MM:SS.fffffffff, a
  # BINARY_FLOAT or BINARY_DOUBLE as text that keeps every digit, and an
  # interval's JSON text as an object.
  script ora-read <<'SH'
docker exec -i "$ORACLE_CONTAINER" sqlplus -s -L system/openetl@//localhost:1521/FREEPDB1 <<SQL
SET DEFINE OFF FEEDBACK OFF LINESIZE 32767 TRIMOUT ON SERVEROUTPUT ON SIZE UNLIMITED FORMAT TRUNCATED
WHENEVER SQLERROR EXIT FAILURE
DECLARE
  c SYS_REFCURSOR;
  v CLOB;
  o JSON_OBJECT_T;
  k JSON_KEY_LIST;
  cols VARCHAR2(32767);
BEGIN
  FOR r IN (SELECT column_name, data_type FROM all_tab_columns
            WHERE owner = '$OPENETL_NAMESPACE' AND table_name = '$OPENETL_STREAM' ORDER BY column_id) LOOP
    cols := cols || CASE WHEN cols IS NOT NULL THEN ', ' END || '''' || REPLACE(r.column_name, '''', '''''') || ''' VALUE ' ||
      REPLACE(CASE WHEN r.data_type LIKE 'INTERVAL DAY%' THEN 'CASE WHEN "c" IS NOT NULL THEN TO_CHAR(EXTRACT(HOUR FROM "c"), ''FM00'') || '':''
        || TO_CHAR(EXTRACT(MINUTE FROM "c"), ''FM00'') || '':'' || TO_CHAR(EXTRACT(SECOND FROM "c"), ''FM00.000000000'') END'
        WHEN r.data_type LIKE 'BINARY_%' THEN 'TO_CHAR("c")' ELSE '"c"' END, '"c"', '"' || r.column_name || '"');
  END LOOP;
  IF cols IS NULL THEN
    RETURN;
  END IF;
  OPEN c FOR 'SELECT JSON_OBJECT(' || cols || ' RETURNING CLOB) FROM "$OPENETL_NAMESPACE"."$OPENETL_STREAM"';
  LOOP
    FETCH c INTO v;
    EXIT WHEN c%NOTFOUND;
    o := JSON_OBJECT_T.parse(v);
    k := o.get_keys;
    FOR i IN 1 .. k.COUNT LOOP
      IF o.get(k(i)).is_string AND o.get_string(k(i)) LIKE '{"months": %' THEN
        o.put(k(i), JSON_OBJECT_T.parse(o.get_string(k(i))));
      END IF;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(o.to_clob);
  END LOOP;
END;
/
SQL
SH
  script ora-drop <<'SH'
docker exec -i "$ORACLE_CONTAINER" sqlplus -s -L system/openetl@//localhost:1521/FREEPDB1 >/dev/null <<SQL
WHENEVER SQLERROR EXIT FAILURE
BEGIN
  EXECUTE IMMEDIATE 'DROP USER "$OPENETL_NAMESPACE" CASCADE';
EXCEPTION WHEN OTHERS THEN
  IF SQLCODE != -1918 THEN RAISE; END IF;
END;
/
SQL
SH
  ora_dest() { printf '{"dsn": "%s"}' "$1"; }
  config ora-dest "$(ora_dest "$(ora_dsn openetl_dest:openetl 1521 '')")"
  config ora-dest-bad "$(ora_dest "$bad_ora")"
  config ora-dest-closed "$(ora_dest "$closed_ora")"
  check destination-oracle oracle --role destination \
    --config "$work/ora-dest.json" \
    --bad-credentials "$work/ora-dest-bad.json" \
    --unreachable "$work/ora-dest-closed.json" \
    --read-command "$work/ora-read" --drop-command "$work/ora-drop" \
    --lossy-type "string=Oracle stores an empty string as null" \
    --lossy-type "binary=Oracle stores an empty binary value as null"
}

# ClickHouse: a MergeTree table read in full and a ReplacingMergeTree read
# incrementally, and tables written in databases of the checks' own, read
# back over HTTP.
checks_clickhouse() {
  export CH_HTTP=${OPENETL_TEST_CLICKHOUSE_HTTP:-http://127.0.0.1:8123}
  local addr=${OPENETL_TEST_CLICKHOUSE_ADDR:-127.0.0.1:9000} q
  ch() { curl -sf -u openetl:openetl "$CH_HTTP" --data-binary "$1"; }
  for q in "CREATE DATABASE $run" \
    "CREATE TABLE $run.users (id Int64, name String, joined Date) ENGINE = MergeTree ORDER BY id" \
    "INSERT INTO $run.users SELECT number + 1, concat('u', toString(number + 1)), toDate('2020-01-01') + number FROM numbers(120)" \
    "CREATE TABLE $run.events (id UInt64, at DateTime64(3, 'UTC'), body String) ENGINE = ReplacingMergeTree ORDER BY id" \
    "INSERT INTO $run.events SELECT number + 1, toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + toIntervalMinute(number), '{}' FROM numbers(90)"; do
    ch "$q" || exit 1
  done
  ch_config() { printf '{"addr": "%s", "user": "openetl", "password": "%s", "database": "%s"%s}' "$1" "$2" "$3" "${4:-}"; }
  script ch-add <<'SH'
case $OPENETL_STREAM in
events)
  curl -sf -u openetl:openetl "$CH_HTTP" --data-binary "INSERT INTO $RUN.events
    SELECT number + toUInt64(now()) * 100, now64(3, 'UTC'), '{}' FROM numbers(5)" && echo 5 ;;
*) echo 0 ;;
esac
SH
  # Reads with FINAL, and a String that isn't UTF-8 as \x and its hex.
  script ch-read <<'SH'
q() {
  curl -sf -u openetl:openetl --data-binary "$1" \
    "$CH_HTTP/?param_ns=$OPENETL_NAMESPACE&param_st=$OPENETL_STREAM&output_format_json_quote_denormals=1"
}
[ "$(q 'EXISTS TABLE {ns:Identifier}.{st:Identifier}')" = 1 ] || exit 0
cols=$(q "$(cat <<'SQL'
SELECT arrayStringConcat(groupArray(if(type IN ('String', 'Nullable(String)'),
    concat('if(isValidUTF8(', c, '), ', c, ', concat(''\\\\x'', lower(hex(', c, '))))'), c) || ' AS ' || c), ', ')
FROM (SELECT type, concat('`', replaceAll(name, '`', '\\`'), '`') AS c FROM system.columns
  WHERE database = {ns:String} AND table = {st:String} ORDER BY position) FORMAT TSVRaw
SQL
)") || exit 1
q "SELECT $cols FROM {ns:Identifier}.{st:Identifier} SETTINGS final = 1 FORMAT JSONEachRow"
SH
  script ch-drop <<'SH'
curl -sf -u openetl:openetl "$CH_HTTP/?param_ns=$OPENETL_NAMESPACE" --data-binary 'DROP DATABASE IF EXISTS {ns:Identifier} SYNC' >/dev/null
SH
  config ch-source "$(ch_config "$addr" openetl "$run" ', "chunkRows": 25')"
  config ch-source-bad "$(ch_config "$addr" not-the-password "$run" ', "chunkRows": 25')"
  config ch-source-closed "$(ch_config 127.0.0.1:1 openetl "$run" ', "chunkRows": 25')"
  check source-clickhouse clickhouse --role source \
    --config "$work/ch-source.json" \
    --bad-credentials "$work/ch-source-bad.json" \
    --unreachable "$work/ch-source-closed.json" \
    --stream users:full_refresh --stream events:incremental:at --add-records-command "$work/ch-add" \
    --stops-at-until
  config ch-dest "$(ch_config "$addr" openetl openetl)"
  config ch-dest-bad "$(ch_config "$addr" not-the-password openetl)"
  config ch-dest-closed "$(ch_config 127.0.0.1:1 openetl openetl)"
  check destination-clickhouse clickhouse --role destination \
    --config "$work/ch-dest.json" \
    --bad-credentials "$work/ch-dest-bad.json" \
    --unreachable "$work/ch-dest-closed.json" \
    --read-command "$work/ch-read" --drop-command "$work/ch-drop" \
    --lossy-type "timestamp=clamped to 1900 to 2262-04-11" --lossy-type "timestamp_tz=clamped to 1900 to 2262-04-11" \
    --lossy-type "timestamp_ns=clamped to 1900 to 2262-04-11" --lossy-type "date=clamped to Date32's range, 1900 to 2299" \
    --lossy-type "interval=a null lands as zero, as a Tuple isn't Nullable" \
    --lossy-type "list=a null lands as an empty Array, as an Array isn't Nullable"
  ch "DROP DATABASE IF EXISTS $run SYNC"
}

# MongoDB: a collection read in full, one incrementally on a field and one
# with change streams. MONGODB_CONTAINER is the MongoDB container, whose
# mongosh adds and reads documents.
checks_mongodb() {
  export MONGODB_CONTAINER=${MONGODB_CONTAINER:-mongodb}
  local mongo=${OPENETL_TEST_MONGODB_URI:-mongodb://openetl:openetl@127.0.0.1:27017/?directConnection=true}
  mongosh_() { docker exec -i "$MONGODB_CONTAINER" mongosh --quiet -u openetl -p openetl --authenticationDatabase admin "$@"; }
  mongosh_ --eval "
    const d = db.getSiblingDB('$run');
    d.users.insertMany(Array.from({length: 120}, (_, i) => ({_id: i + 1, name: 'u' + (i + 1), joined: new Date(Date.UTC(2020, 0, 2 + i))})));
    d.events.insertMany(Array.from({length: 90}, (_, i) => ({at: new Date(Date.UTC(2026, 0, 1, 0, i)), body: {n: i}})));
    d.orders.insertMany(Array.from({length: 70}, (_, i) => ({_id: i + 1, total: (i + 1) * 1.25})));" >/dev/null || exit 1
  mongo_source() { printf '{"uri": "%s", "database": "%s", "chunkDocuments": 25}' "$1" "$run"; }
  script mongo-add <<'SH'
docker exec "$MONGODB_CONTAINER" mongosh --quiet -u openetl -p openetl --authenticationDatabase admin --eval "
  const d = db.getSiblingDB('$RUN');
  switch ('$OPENETL_STREAM') {
  case 'orders':
    const top = d.orders.find().sort({_id: -1}).limit(1).toArray()[0]._id;
    d.orders.insertMany([1, 2, 3, 4, 5].map(i => ({_id: top + i, total: 2.5})));
    print(5);
    break;
  case 'events':
    d.events.insertMany([1, 2, 3, 4, 5].map(i => ({at: new Date(Date.now() + i), body: {}})));
    print(5);
    break;
  default:
    print(0);
  }"
SH
  # Prints a collection's documents as JSON lines: BSON's dates in RFC 3339,
  # numbers past JSON's as text, UUIDs as text and other binaries in base64.
  script mongo-read <<'SH'
docker exec "$MONGODB_CONTAINER" mongosh --quiet -u openetl -p openetl --authenticationDatabase admin --eval "
  function plain(v) {
    if (v === null || v === undefined) return null;
    if (v instanceof Date) return v.toISOString();
    if (v._bsontype === 'Binary') return v.sub_type === 4 ? v.toUUID().toHexString() : v.toString('base64');
    if (['Decimal128', 'Long', 'Int32', 'Double'].includes(v._bsontype)) return v.toString();
    if (v._bsontype === 'ObjectId') return v.toHexString();
    if (typeof v === 'number') return Number.isFinite(v) ? v : String(v);
    if (Array.isArray(v)) return v.map(plain);
    if (typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, plain(x)]));
    return v;
  }
  db.getSiblingDB('$OPENETL_NAMESPACE').getCollection('$OPENETL_STREAM').find().forEach(d => print(JSON.stringify(plain(d))));"
SH
  script mongo-drop <<'SH'
docker exec "$MONGODB_CONTAINER" mongosh --quiet -u openetl -p openetl --authenticationDatabase admin \
  --eval "db.getSiblingDB('$OPENETL_NAMESPACE').dropDatabase()" >/dev/null
SH
  local bad=${mongo/:openetl@/:not-the-password@}
  local closed="mongodb://openetl:openetl@127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=2000"
  config mongo-source "$(mongo_source "$mongo")"
  config mongo-source-bad "$(mongo_source "$bad")"
  config mongo-source-closed "$(mongo_source "$closed")"
  check source-mongodb mongodb --role source \
    --config "$work/mongo-source.json" \
    --bad-credentials "$work/mongo-source-bad.json" \
    --unreachable "$work/mongo-source-closed.json" \
    --stream users:full_refresh --stream events:incremental:at --stream orders:cdc \
    --add-records-command "$work/mongo-add" --stops-at-until
  config mongo-dest "{\"uri\": \"$mongo\", \"database\": \"$run\"}"
  config mongo-dest-bad "{\"uri\": \"$bad\", \"database\": \"$run\"}"
  config mongo-dest-closed "{\"uri\": \"$closed\", \"database\": \"$run\"}"
  check destination-mongodb mongodb --role destination \
    --config "$work/mongo-dest.json" \
    --bad-credentials "$work/mongo-dest-bad.json" \
    --unreachable "$work/mongo-dest-closed.json" \
    --read-command "$work/mongo-read" --drop-command "$work/mongo-drop" \
    --untyped --lossy-type "timestamp_tz=BSON dates keep milliseconds" --lossy-type "timestamp=BSON dates keep milliseconds"
  mongosh_ --eval "db.getSiblingDB('$run').dropDatabase()" >/dev/null
}

# DynamoDB: a table read in full, one read incrementally, and one read with
# cdc from its stream, in DynamoDB Local.
checks_dynamodb() {
  export DYNAMODB=${OPENETL_TEST_DYNAMODB_ENDPOINT:-http://127.0.0.1:8000}
  ddb() { aws --endpoint-url "$DYNAMODB" dynamodb "$@"; }
  # items TABLE FIRST COUNT JQ puts COUNT items, each JQ of i, 25 a request.
  items() {
    local from=$2 to=$(($2 + $3)) i
    for ((i = from; i < to; i += 25)); do
      jq -nc --arg t "$1" --argjson a "$i" --argjson b "$((i + 25 < to ? i + 25 : to))" \
        "{(\$t): [range(\$a; \$b) | {PutRequest: {Item: ($4)}}]}" >"$work/ddb-items.json" &&
        ddb batch-write-item --request-items "file://$work/ddb-items.json" >/dev/null || return 1
    done
  }
  ddb create-table --table-name "$run-users" --billing-mode PAY_PER_REQUEST \
    --attribute-definitions AttributeName=id,AttributeType=N --key-schema AttributeName=id,KeyType=HASH >/dev/null &&
    ddb create-table --table-name "$run-events" --billing-mode PAY_PER_REQUEST \
      --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=N \
      --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE >/dev/null &&
    ddb create-table --table-name "$run-orders" --billing-mode PAY_PER_REQUEST \
      --attribute-definitions AttributeName=id,AttributeType=N --key-schema AttributeName=id,KeyType=HASH \
      --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES >/dev/null || exit 1
  items "$run-users" 1 120 '{id: {N: "\(.)"}, name: {S: "u\(.)"}}' &&
    items "$run-events" 1 90 '{pk: {S: "p\(. % 7)"}, sk: {N: "\(.)"}, at: {N: "\(1000 + .)"}}' &&
    items "$run-orders" 1 70 '{id: {N: "\(.)"}, total: {N: "\(.).25"}}' || exit 1
  script ddb-add <<'SH'
n=$(aws --endpoint-url "$DYNAMODB" dynamodb scan --table-name "$OPENETL_STREAM" --select COUNT --output text --query Count) || exit 1
case $OPENETL_STREAM in
*-orders) item='{id: {N: "\(.)"}, total: {N: "2.5"}}' ;;
*-events) item='{pk: {S: "new"}, sk: {N: "\(.)"}, at: {N: "\(100000 + .)"}}' ;;
*) echo 0; exit 0 ;;
esac
jq -nc --arg t "$OPENETL_STREAM" --argjson a "$((n + 1000))" "{(\$t): [range(\$a; \$a + 5) | {PutRequest: {Item: ($item)}}]}" >"$WORK/ddb-add.json" &&
  aws --endpoint-url "$DYNAMODB" dynamodb batch-write-item --request-items "file://$WORK/ddb-add.json" >/dev/null && echo 5
SH
  # ddb-read prints a table's items as JSON, numbers with every digit.
  script ddb-read <<'SH'
aws --endpoint-url "$DYNAMODB" dynamodb scan --table-name "$OPENETL_NAMESPACE.$OPENETL_STREAM" --consistent-read --output json 2>/dev/null |
  jq -r 'def obj(f): "{" + (to_entries | map((.key | tojson) + ":" + (.value | f)) | join(",")) + "}";
    def enc: if has("N") then .N elif has("NS") then "[" + (.NS | join(",")) + "]"
      elif has("M") then .M | obj(enc) elif has("L") then "[" + (.L | map(enc) | join(",")) + "]"
      elif has("BOOL") then .BOOL | tostring elif has("NULL") then "null" else (.S // .B // .SS // .BS) | tojson end;
    .Items[] | obj(enc)'
exit 0
SH
  script ddb-drop <<'SH'
for t in $(aws --endpoint-url "$DYNAMODB" dynamodb list-tables --output text --query 'TableNames[]'); do
  case $t in "$OPENETL_NAMESPACE".*) aws --endpoint-url "$DYNAMODB" dynamodb delete-table --table-name "$t" >/dev/null ;; esac
done
SH
  ddb_config() { printf '{"region": "us-east-1", "endpoint": "%s", "accessKeyId": "%s", "secretAccessKey": "openetl"%s}' "$1" "$2" "$3"; }
  tables=", \"tables\": [\"$run-users\", \"$run-events\", \"$run-orders\"], \"scanLimit\": 25, \"segments\": 3"
  # DynamoDB Local takes any key of letters and digits.
  config ddb-source "$(ddb_config "$DYNAMODB" openetl "$tables")"
  config ddb-source-bad "$(ddb_config "$DYNAMODB" not-a-key! "$tables")"
  config ddb-source-closed "$(ddb_config http://127.0.0.1:1 openetl "$tables")"
  check source-dynamodb dynamodb --role source \
    --config "$work/ddb-source.json" \
    --bad-credentials "$work/ddb-source-bad.json" \
    --unreachable "$work/ddb-source-closed.json" \
    --stream "$run-users:full_refresh" --stream "$run-events:incremental:at" --stream "$run-orders:cdc" \
    --add-records-command "$work/ddb-add" --stops-at-until
  config ddb-dest "$(ddb_config "$DYNAMODB" openetl "")"
  config ddb-dest-bad "$(ddb_config "$DYNAMODB" not-a-key! "")"
  config ddb-dest-closed "$(ddb_config http://127.0.0.1:1 openetl "")"
  check destination-dynamodb dynamodb --role destination \
    --config "$work/ddb-dest.json" \
    --bad-credentials "$work/ddb-dest-bad.json" \
    --unreachable "$work/ddb-dest-closed.json" \
    --read-command "$work/ddb-read" --drop-command "$work/ddb-drop" --untyped
  for t in "$run-users" "$run-events" "$run-orders"; do ddb delete-table --table-name "$t" >/dev/null 2>&1; done
}

# Kafka: two topics of JSON records.
checks_kafka() {
  produce() { # topic, first id, count
    seq "$2" $(($2 + $3 - 1)) | sed 's/.*/{"id":&,"name":"n"}/' |
      docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic "$1" >/dev/null
  }
  for t in a:3 b:1; do
    docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
      --topic "$run.${t%:*}" --partitions "${t#*:}" >/dev/null || exit 1
  done
  produce "$run.a" 1 150 && produce "$run.b" 1 30 || exit 1
  script kafka-add <<'SH'
n=$(date +%s)
seq 1 5 | sed "s/.*/{\"id\":${n}00&,\"name\":\"n\"}/" |
  docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic "$OPENETL_STREAM" >/dev/null && echo 5
SH
  config kafka-source "{\"brokers\": [\"$kafka\"], \"topics\": [\"$run.a\", \"$run.b\"]}"
  config kafka-source-closed "{\"brokers\": [\"127.0.0.1:1\"], \"topics\": [\"$run.a\", \"$run.b\"]}"
  # The SASL_SSL broker, with a password it rejects.
  kafka_rejected() { printf '{"brokers": ["%s"], "tls": true, "tlsInsecureSkipVerify": true, "saslMechanism": "scram-sha-512", "saslUsername": "scram", "saslPassword": "wrong"%s}' "$kafka_sasl" "$1"; }
  config kafka-source-rejected "$(kafka_rejected ", \"topics\": [\"$run.a\", \"$run.b\"]")"
  check source-kafka kafka --role source \
    --config "$work/kafka-source.json" \
    --unreachable "$work/kafka-source-closed.json" \
    --bad-credentials "$work/kafka-source-rejected.json" \
    --add-records-command "$work/kafka-add" \
    $(stops kafka --stops-at-until --stops-at-completion)
  config kafka-dest "{\"brokers\": [\"$kafka\"]}"
  config kafka-dest-closed '{"brokers": ["127.0.0.1:1"]}'
  config kafka-dest-rejected "$(kafka_rejected "")"
  check destination-kafka kafka --role destination \
    --config "$work/kafka-dest.json" \
    --unreachable "$work/kafka-dest-closed.json" \
    --bad-credentials "$work/kafka-dest-rejected.json"
}

# Kinesis: two streams of JSON records.
checks_kinesis() {
  records() { # first id, count
    local out="[" i
    for ((i = $1; i < $1 + $2; i++)); do
      out+="{\"Data\": \"{\\\"id\\\":$i,\\\"name\\\":\\\"n\\\"}\", \"PartitionKey\": \"k$((i % 10))\"},"
    done
    printf '%s' "${out%,}]"
  }
  kinesis_cli() { aws --endpoint-url "$kinesis" --cli-binary-format raw-in-base64-out kinesis "$@"; }
  for s in a:2 b:1; do
    kinesis_cli create-stream --stream-name "$run-${s%:*}" --shard-count "${s#*:}" &&
      kinesis_cli wait stream-exists --stream-name "$run-${s%:*}" || exit 1
  done
  records 0 100 >"$work/kinesis-a.json" && records 0 40 >"$work/kinesis-b.json"
  kinesis_cli put-records --stream-name "$run-a" --records "file://$work/kinesis-a.json" >/dev/null &&
    kinesis_cli put-records --stream-name "$run-b" --records "file://$work/kinesis-b.json" >/dev/null || exit 1
  kinesis_config() { printf '{"region": "us-east-1", "endpoint": "%s", "accessKeyId": "openetl", "secretAccessKey": "openetl"%s}' "$1" "$2"; }
  streams=", \"streams\": [\"$run-a\", \"$run-b\"]"
  script kinesis-add <<'SH'
n=$(date +%s)
recs=
for i in 1 2 3 4 5; do
  recs="$recs${recs:+,}{\"Data\": \"{\\\"id\\\":${n}00$i}\", \"PartitionKey\": \"k$i\"}"
done
echo "[$recs]" >"$WORK/add-$OPENETL_STREAM.json"
aws --endpoint-url "$KINESIS" --cli-binary-format raw-in-base64-out kinesis put-records \
  --stream-name "$OPENETL_STREAM" --records "file://$WORK/add-$OPENETL_STREAM.json" >/dev/null && echo 5
SH
  config kinesis-source "$(kinesis_config "$kinesis" "$streams")"
  config kinesis-source-closed "$(kinesis_config http://127.0.0.1:1 "$streams")"
  check source-kinesis kinesis --role source \
    --config "$work/kinesis-source.json" \
    --unreachable "$work/kinesis-source-closed.json" \
    --add-records-command "$work/kinesis-add"
  config kinesis-dest "$(kinesis_config "$kinesis" "")"
  config kinesis-dest-closed "$(kinesis_config http://127.0.0.1:1 "")"
  check destination-kinesis kinesis --role destination \
    --config "$work/kinesis-dest.json" \
    --unreachable "$work/kinesis-dest-closed.json"
}

# files_checks STORE PACKAGE: the files source of CSV and JSON Lines files,
# and the destination, of PACKAGE on STORE: s3, gcs or azure.
files_checks() {
  local store=$1 pkg=$2
  local gcs=${OPENETL_TEST_GCS_ENDPOINT:-http://127.0.0.1:4443}
  local azure=${OPENETL_TEST_AZURE_ENDPOINT:-http://127.0.0.1:10000/devstoreaccount1}
  # Azurite's well-known account key.
  local azure_key=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
  export S3=$s3 GCS=$gcs AZURE=$azure AZURE_KEY=$azure_key
  # put STORE KEY FILE uploads FILE to the bucket packages, or creates the
  # bucket without FILE; Azure's request is signed with its shared key.
  { echo '#!/usr/bin/env bash'; cat; } >"$work/put" <<'SH'
set -eo pipefail
case $1 in
# A bucket that's there already answers 409, and is fine.
s3) [ -z "$3" ] && exec curl -s -o /dev/null -X PUT "$S3/packages"
  exec curl -sf -o /dev/null -X PUT --data-binary "@$3" "$S3/packages/$2" ;;
gcs) [ -z "$3" ] && { curl -s -o /dev/null -X POST -H 'Content-Type: application/json' -d '{"name":"packages"}' "$GCS/storage/v1/b?project=openetl"; exit 0; }
  exec curl -sf -o /dev/null -X POST --data-binary "@$3" "$GCS/upload/storage/v1/b/packages/o?uploadType=media&name=$2" ;;
azure)
  account=${AZURE##*/} date=$(LC_ALL=C TZ=GMT date '+%a, %d %b %Y %H:%M:%S GMT')
  if [ -z "$3" ]; then
    url="$AZURE/packages?restype=container" len="" type="" blob="" resource="/$account/$account/packages"$'\n'"restype:container" data=()
  else
    url="$AZURE/packages/$2" len=$(wc -c <"$3" | tr -d ' ') type=application/octet-stream blob=$'x-ms-blob-type:BlockBlob\n' resource="/$account/$account/packages/$2" data=(--data-binary "@$3")
  fi
  sign="PUT"$'\n\n\n'"$len"$'\n\n'"$type"$'\n\n\n\n\n\n\n'"${blob}x-ms-date:$date"$'\n'"x-ms-version:2021-08-06"$'\n'"$resource"
  sig=$(printf '%s' "$sign" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$(printf '%s' "$AZURE_KEY" | base64 -d | od -An -tx1 | tr -d " \n")" -binary | base64)
  status=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "${data[@]}" ${blob:+-H "x-ms-blob-type: BlockBlob" -H "Content-Type: $type"} \
    -H "x-ms-date: $date" -H "x-ms-version: 2021-08-06" -H "Authorization: SharedKey $account:$sig" "$url")
  # 409: the container is there already.
  [[ $status == 201 || ( -z "$3" && $status == 409 ) ]] ;;
esac
SH
  chmod +x "$work/put"
  # add writes a file of 5 records to $OPENETL_STREAM's directory in $STORE.
  script files-add <<'SH'
f=$WORK/add-$(date +%s)-$$
case $OPENETL_STREAM in
people) { echo id,name,born; for i in 1 2 3 4 5; do echo "$i,p$i,2026-01-0$i"; done; } | gzip >"$f"; key=people/$(basename "$f").csv.gz ;;
*) for i in 1 2 3 4 5; do echo "{\"id\":$i,\"level\":\"info\"}"; done >"$f"; key=logs/$(basename "$f").jsonl ;;
esac
"$WORK/put" "$STORE" "$RUN/$key" "$f" && echo 5
SH
  for i in 1 2 3; do
    printf 'id,name,born\n%d,"a, %d",2026-01-0%d\n%d,,2026-01-0%d\n' "$i" "$i" "$i" "$((i + 10))" "$i" | gzip >"$work/people-$i.csv.gz"
    printf '{"id":%d,"level":"warn","attrs":{"n":%d}}\n{"id":%d,"level":null}\n' "$i" "$i" "$((i + 10))" >"$work/logs-$i.jsonl"
  done
  local url endpoint creds closed
  case $store in
  s3) url=s3://packages/$run endpoint=$s3 closed=http://127.0.0.1:1
    creds='"region": "us-east-1", "accessKeyId": "openetl", "secretAccessKey": "openetl"' ;;
  gcs) url=gs://packages/$run endpoint=$gcs closed=http://127.0.0.1:1 creds="" ;;
  azure) url=azblob://packages/$run endpoint=$azure closed=http://127.0.0.1:1/devstoreaccount1
    creds="\"accountName\": \"devstoreaccount1\", \"accountKey\": \"$azure_key\"" ;;
  esac
  closed="\"endpoint\": \"$closed\"${creds:+, $creds}"
  creds="\"endpoint\": \"$endpoint\"${creds:+, $creds}"
  "$work/put" "$store" || exit 1
  for i in 1 2 3; do
    "$work/put" "$store" "$run/people/$i.csv.gz" "$work/people-$i.csv.gz" &&
      "$work/put" "$store" "$run/logs/$i.jsonl" "$work/logs-$i.jsonl" || exit 1
  done
  streams='"streams": [{"name": "people", "glob": "people/*.csv.gz", "format": "csv", "columns": {"born": "date"}}, {"name": "logs", "glob": "logs/**", "format": "jsonl"}]'
  config "$store-source" "{\"url\": \"$url/\", $creds, $streams}"
  config "$store-source-closed" "{\"url\": \"$url/\", $closed, $streams}"
  config "$store-dest" "{\"url\": \"$url/dest\", $creds}"
  config "$store-dest-closed" "{\"url\": \"$url/dest\", $closed}"
  # Only Azurite checks credentials.
  local source_bad=() dest_bad=()
  if [[ $store == azure ]]; then
    local wrong
    wrong=$(printf 'A%.0s' $(seq 86))==
    config azure-source-bad "{\"url\": \"$url/\", ${creds/$azure_key/$wrong}, $streams}"
    config azure-dest-bad "{\"url\": \"$url/dest\", ${creds/$azure_key/$wrong}}"
    source_bad=(--bad-credentials "$work/azure-source-bad.json")
    dest_bad=(--bad-credentials "$work/azure-dest-bad.json")
  fi
  export STORE=$store
  check "source-$pkg" "$pkg" --role source \
    --config "$work/$store-source.json" \
    --unreachable "$work/$store-source-closed.json" ${source_bad[@]+"${source_bad[@]}"} \
    --stream people:incremental --stream logs:incremental --add-records-command "$work/files-add" \
    --stops-at-until
  check "destination-$pkg" "$pkg" --role destination \
    --config "$work/$store-dest.json" \
    --unreachable "$work/$store-dest-closed.json" ${dest_bad[@]+"${dest_bad[@]}"}
}

checks_s3() { files_checks s3 s3; }
checks_gcs() { files_checks gcs gcs; }
checks_azureblob() { files_checks azure azureblob; }

# Iceberg: tables through the REST catalog, in the S3 store's iceberg bucket.
checks_iceberg() {
  iceberg_config() { printf '{"uri": "%s", "s3Endpoint": "%s", "region": "us-east-1", "accessKeyId": "openetl", "secretAccessKey": "openetl"}' "$1" "$s3"; }
  config iceberg "$(iceberg_config "$iceberg")"
  config iceberg-closed "$(iceberg_config http://127.0.0.1:1)"
  check destination-iceberg iceberg --role destination \
    --config "$work/iceberg.json" \
    --unreachable "$work/iceberg-closed.json"
}

for name in "${names[@]}"; do
  if declare -F "checks_$name" >/dev/null; then
    "checks_$name"
  else
    echo "::error::$0 has no checks for $name: add checks_$name"
    failed+=("$name")
  fi
done

if ((${#failed[@]})); then
  echo "failed: ${failed[*]}"
  exit 1
fi
