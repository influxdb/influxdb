#!/bin/bash

set -eu -o pipefail

args=( "$@" )
for i in "${!args[@]}"; do
    args[i]="$(echo "${args[$i]}" | envsubst)"
done

exec "$PACKAGE" "${args[@]}"
