#!/usr/bin/env bash
set -exo pipefail

declare -a DOWNGRADE_TARGETS=('2.0' '2.1')

declare -r TEST_ORG=testorg
declare -r TEST_TOKEN=supersecretfaketoken
declare -r INIT_PING_ATTEMPTS=600

function download_older_binary () {
    local -r target_version=$1 dl_dir=$2

    local dl_url
    local dl_sha
    case ${target_version} in
        2.0)
            dl_url=https://dl.influxdata.com/influxdb/releases/influxdb2-2.0.9-linux-amd64.tar.gz
            dl_sha=64b9cfea1b5ca07479a16332056f9e7f806ad71df9c6cc0ec70c4333372b9d26
            ;;
        2.1)
            dl_url=https://dl.influxdata.com/influxdb/releases/influxdb2-2.1.1-linux-amd64.tar.gz
            dl_sha=1688e3afa7f875d472768e4f4f5a909b357287a45a8f28287021e4184a185927
            ;;
        *)
            >&2 echo Error: Unknown downgrade target "'$target_version'"
            exit 1
            ;;
    esac

    local -r archive="influxd-${target_version}.tar.gz"
    curl -sL -o "${dl_dir}/${archive}" "$dl_url"
    echo "${dl_sha}  ${dl_dir}/${archive}" | sha256sum -c --
    tar xzf "${dl_dir}/${archive}" -C "$dl_dir" --strip-components=1
    rm "${dl_dir}/${archive}"
    mv "${dl_dir}/influxd" "${dl_dir}/influxd-${target_version}"
}

function test_downgrade_target () {
    local -r influxd_path=$1 target_version=$2 tmp=$3

    download_older_binary "$target_version" "$tmp"

    local -r bolt_path="${tmp}/influxd-${target_version}.bolt"
    local -r sqlite_path="${tmp}/influxd-${target_version}.sqlite"

    cp "${tmp}/influxd.bolt" "$bolt_path"
    cp "${tmp}/influxd.sqlite" "$sqlite_path"

    "$influxd_path" downgrade \
        --bolt-path "$bolt_path" \
        --sqlite-path "$sqlite_path" \
        "$target_version"

    INFLUXD_BOLT_PATH="$bolt_path" INFLUXD_SQLITE_PATH="$sqlite_path" INFLUXD_ENGINE_PATH="${tmp}/engine" \
        "${tmp}/influxd-${target_version}" &
    local -r influxd_pid="$!"

    wait_for_influxd "$influxd_pid"

    if [[ "$(curl -s -o /dev/null -H "Authorization: Token $TEST_TOKEN" "http://localhost:8086/api/v2/me" -w "%{http_code}")" != "200" ]]; then
        >&2 echo Error: "Downgraded DB doesn't recognize auth token"
        exit 1
    fi

    kill -TERM "$influxd_pid"
    wait "$influxd_pid" || true
}

function wait_for_influxd () {
    local -r influxd_pid=$1
    local ping_count=0
    while kill -0 "${influxd_pid}" && [ ${ping_count} -lt ${INIT_PING_ATTEMPTS} ]; do
        sleep 1
        ping_count=$((ping_count+1))
        if [[ "$(curl -s -o /dev/null "http://localhost:8086/health" -w "%{http_code}")" = "200" ]]; then
            return
        fi
    done
    if [ ${ping_count} -eq ${INIT_PING_ATTEMPTS} ]; then
        >&2 echo influxd took too long to start up
    else
        >&2 echo influxd crashed during startup
    fi
    return 1
}

function setup_influxd () {
    local -r influxd_path=$1 tmp=$2
    INFLUXD_BOLT_PATH="${tmp}/influxd.bolt" INFLUXD_SQLITE_PATH="${tmp}/influxd.sqlite" INFLUXD_ENGINE_PATH="${tmp}/engine" \
        "$influxd_path" &
    local -r influxd_pid="$!"

    wait_for_influxd "$influxd_pid"
    curl -s -o /dev/null -XPOST \
        -d '{"username":"default","password":"fakepassword","org":"'$TEST_ORG'","bucket":"unused","token":"'$TEST_TOKEN'"}' \
        http://localhost:8086/api/v2/setup

    kill -TERM "$influxd_pid"
    wait "$influxd_pid" || true
}

function main () {
    if [[ $# != 1 ]]; then
        >&2 echo Usage: $0 '<influxd-path>'
        exit 1
    fi
    local -r influxd_path=$1

    local -r tmp="$(mktemp -d -t "test-downgrade-${target_version}-XXXXXX")"
    trap "rm -rf ${tmp}" EXIT

    setup_influxd "$influxd_path" "$tmp"

    for target in ${DOWNGRADE_TARGETS[@]}; do
        test_downgrade_target "$influxd_path" "$target" "$tmp"
    done
}

main "${@}"
