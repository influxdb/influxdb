#!/usr/bin/env bash

# This script is used to download built UI assets from the "influxdata/ui"
# repository. The built UI assets are attached to a release in "influxdata/ui",
# which is linked here.
#
# The master branch of "influxdata/influxdb" (this repository) downloads from the
# release tagged at the latest released version of influxdb.
# For example, if master is tracking slightly ahead of 2.6.1, then the tag would be OSS-v2.6.1.
#
# Feature branches of "influxdata/influxdb" (2.0, 2.1, etc) download from their
# respective releases in "influxdata/ui" (OSS-2.0, OSS-2.1, etc). Those releases
# are updated only when a bug fix needs included for the UI of that OSS release.

set -e

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r ROOT_DIR=$(dirname ${SCRIPT_DIR})
declare -r STATIC_DIR="$ROOT_DIR/static"

UI_RELEASE="OSS-v2.7.10"

# Download the SHA256 checksum attached to the release. To verify the integrity
# of the download, this checksum will be used to check the download tar file
# containing the built UI assets.
curl -Ls https://github.com/influxdata/ui/releases/download/$UI_RELEASE/sha256.txt --output sha256.txt

# Download the tar file containing the built UI assets.
curl -L https://github.com/influxdata/ui/releases/download/$UI_RELEASE/build.tar.gz --output build.tar.gz

# Verify the checksums match; exit if they don't.
case "$(uname -s)" in
    FreeBSD | Darwin)
        echo "$(cat sha256.txt)" | shasum --algorithm 256 -c \
            || { echo "Checksums did not match for downloaded UI assets!"; exit 1; } ;;
    Linux)
        echo "$(cat sha256.txt)" | sha256sum -c -- \
            || { echo "Checksums did not match for downloaded UI assets!"; exit 1; } ;;
    *)
        echo "The '$(uname -s)' operating system is not supported as a build host for the UI" >&2
        exit 1
esac

# Extract the assets and clean up.
mkdir -p "$STATIC_DIR/data"
tar -xzf build.tar.gz -C "$STATIC_DIR/data"
rm sha256.txt
rm build.tar.gz
