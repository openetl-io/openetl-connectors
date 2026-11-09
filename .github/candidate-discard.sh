#!/usr/bin/env bash
# Deletes a release candidate, from the repository's root: each package's
# version in the registry, its index card when that was the package's only
# version, and its draft release. Needs gh with write access to the repository
# and to its packages. It refuses a version that's been published.
set -euo pipefail

version=${1:?usage: candidate-discard.sh VERSION}
repo=${GITHUB_REPOSITORY:-openetl-io/openetl-connectors}
org=${repo%%/*}
namespace=${repo#*/}

die() {
  echo "candidate-discard: $*" >&2
  exit 1
}

# remove PACKAGE TAG deletes the version of a container package holding TAG, or
# the whole package when that's its only version, as the registry refuses to
# delete a package's last version. It says whether it deleted the package.
remove() {
  local path="/orgs/$org/packages/container/${1//\//%2F}" versions id
  if ! versions=$(gh api --paginate "$path/versions" --jq '.[]' 2>&1); then
    grep -q 'HTTP 404' <<<"$versions" || die "listing $1: $versions"
    echo "no package $1"
    return 0
  fi
  id=$(jq -rs --arg t "$2" 'map(select(.metadata.container.tags | index($t))) | .[0].id // ""' <<<"$versions")
  if [ -z "$id" ]; then
    echo "$1 has no version tagged $2"
    return 0
  fi
  if [ "$(jq -s length <<<"$versions")" = 1 ]; then
    echo "deleting package $1"
    gh api -X DELETE "$path"
    return 0
  fi
  echo "deleting $1:$2"
  gh api -X DELETE "$path/versions/$id"
}

packages=$(dirname */openetl-connector.yaml)
for p in $packages; do
  [ "$(gh release view "$p/v$version" -R "$repo" --json isDraft --jq .isDraft 2>/dev/null || echo none)" != false ] ||
    die "$p/v$version is published, so it isn't a candidate"
done

# The packages first: if deleting one fails, the drafts are there to retry from.
for p in $packages; do
  remove "$namespace/$p" "$version"
  remove "$namespace/index" "$p"
done
for p in $packages; do
  if [ "$(gh release view "$p/v$version" -R "$repo" --json isDraft --jq .isDraft 2>/dev/null || echo none)" = true ]; then
    gh release delete "$p/v$version" -R "$repo" --yes
  fi
done
