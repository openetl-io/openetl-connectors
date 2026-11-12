#!/usr/bin/env bash
# Packages connectors for this machine's platform, as a release does, and
# checks each role of what it wrote with openetl connector test, against the
# services the integration tests use. The destinations' checks that write need
# a way to read them back, which Postgres has here (psql); the integration
# tests run those checks on every destination.
#
#   test-packages.sh                 every package in this checkout
#   test-packages.sh postgres ...    those packages in this checkout
#   test-packages.sh postgres=TARGET ...
#                                    those packages at TARGET, packaging
#                                    nothing, such as a released version:
#                                    oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.1.0-alpha
#
# A package's checks are the function checks_NAME below, and need only that
# package's services. OPENETL is the openetl CLI. KAFKA_CONTAINER is the Kafka
# container, whose scripts produce records.
set -uo pipefail

openetl=${OPENETL:?the openetl CLI}
export KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
export PG=${OPENETL_TEST_POSTGRES_DSN:-postgres://openetl:openetl@127.0.0.1:5432/openetl}
export PGTZ=UTC
kafka=${OPENETL_TEST_KAFKA_BROKERS:-127.0.0.1:9092}
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
# config NAME JSON writes the config $work/NAME.json.
config() { printf '%s' "$2" >"$work/$1.json"; }
# script NAME writes stdin as an executable script NAME.
script() { { echo '#!/bin/sh'; cat; } >"$work/$1" && chmod +x "$work/$1"; }

# Postgres: a table read in full and one read with cdc.
checks_postgres() {
  psql "$PG" -v ON_ERROR_STOP=1 -q <<SQL || exit 1
CREATE SCHEMA $run;
CREATE TABLE $run.users (id bigint PRIMARY KEY, name text, joined date);
INSERT INTO $run.users SELECT i, 'u' || i, date '2020-01-01' + i FROM generate_series(1, 120) i;
CREATE TABLE $run.orders (id bigint PRIMARY KEY, total numeric(10, 2), placed timestamptz);
INSERT INTO $run.orders SELECT i, i * 1.25, timestamptz '2026-03-01 00:00Z' + i * interval '1 hour' FROM generate_series(1, 70) i;
SQL
  pg_source() { printf '{"dsn": "%s", "schema": "%s", "chunkRows": 25, "snapshotChunkRows": 20, "publication": "%s"}' "$1" "$run" "$run"; }
  bad_pg=${PG/:openetl@/:not-the-password@}
  closed_pg="postgres://openetl:openetl@127.0.0.1:1/openetl?connect_timeout=2"
  script pg-add <<'SH'
case $OPENETL_STREAM in
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
    --stream users:full_refresh --stream orders:cdc --add-records-command "$work/pg-add"
  config pg-dest "{\"dsn\": \"$PG\"}"
  config pg-dest-bad "{\"dsn\": \"$bad_pg\"}"
  config pg-dest-closed "{\"dsn\": \"$closed_pg\"}"
  check destination-postgres postgres --role destination \
    --config "$work/pg-dest.json" \
    --bad-credentials "$work/pg-dest-bad.json" \
    --unreachable "$work/pg-dest-closed.json" \
    --read-command "$work/pg-read" --drop-command "$work/pg-drop"
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
  check source-kafka kafka --role source \
    --config "$work/kafka-source.json" \
    --unreachable "$work/kafka-source-closed.json" \
    --add-records-command "$work/kafka-add"
  config kafka-dest "{\"brokers\": [\"$kafka\"]}"
  config kafka-dest-closed '{"brokers": ["127.0.0.1:1"]}'
  check destination-kafka kafka --role destination \
    --config "$work/kafka-dest.json" \
    --unreachable "$work/kafka-dest-closed.json"
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

# S3: Parquet files in a bucket of the S3 store.
checks_s3() {
  curl -s -o /dev/null -X PUT "$s3/packages"
  s3_config() { printf '{"url": "s3://packages/%s", "endpoint": "%s", "region": "us-east-1", "accessKeyId": "openetl", "secretAccessKey": "openetl"}' "$run" "$1"; }
  config s3 "$(s3_config "$s3")"
  config s3-closed "$(s3_config http://127.0.0.1:1)"
  check destination-s3 s3 --role destination \
    --config "$work/s3.json" \
    --unreachable "$work/s3-closed.json"
}

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
