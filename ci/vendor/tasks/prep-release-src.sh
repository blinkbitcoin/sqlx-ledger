#!/bin/bash

#! Auto synced from Shared CI Resources repository
#! Don't change this file, instead change it in github.com/blinkbitcoin/concourse-shared

set -eu

# ------------ CHANGELOG ------------

pushd repo

# First time
if [[ $(cat ../version/version) == "0.0.0" ]]; then
  echo "  --> creating changelog for all commits"
  git cliff --config ../pipeline-tasks/ci/vendor/config/git-cliff.toml > ../artifacts/gh-release-notes.md

# Fetch changelog from last ref
else
  prev_ref=$(git rev-list -n 1 "$(cat ../version/version)")
  export prev_ref
  new_ref=$(git rev-parse HEAD)
  export new_ref
  echo "  --> creating changelog for $prev_ref..$new_ref"
  git cliff --config ../pipeline-tasks/ci/vendor/config/git-cliff.toml $prev_ref..$new_ref > ../artifacts/gh-release-notes.md
fi

popd

# Generate Changelog
echo "CHANGELOG:"
echo "-------------------------------"
cat artifacts/gh-release-notes.md
echo "-------------------------------"

# ------------ BUMP VERSION ------------

echo -n "Prev Version: "
cat version/version
echo ""

 # Initial Version
if [[ $(cat version/version) == "0.0.0" ]]; then
  echo "0.1.0" > version/version
# Figure out proper version to release
elif [[ $(grep -i Breaking artifacts/gh-release-notes.md) != '' ]]; then  echo "Breaking change found, bumping major version..."
  bump2version major --current-version "$(< version/version)" --allow-dirty version/version
elif [[ $(grep -i Feature artifacts/gh-release-notes.md) != '' ]]; then
  echo "Feature Addition found, bumping minor version..."
  bump2version minor --current-version "$(< version/version)" --allow-dirty version/version
else
  echo "Only patches and fixes found - bumping patch version..."
  bump2version patch --current-version "$(< version/version)" --allow-dirty version/version
fi
echo -n "Release Version: "
cat version/version
echo ""

# ------------ ARTIFACTS ------------

cat version/version > artifacts/gh-release-tag
echo "v$(cat version/version) Release" > artifacts/gh-release-name
