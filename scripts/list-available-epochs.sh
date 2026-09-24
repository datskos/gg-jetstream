#!/usr/bin/env bash
# Check recent Old Faithful epochs without downloading archives or indexes.
set -euo pipefail

usage() {
    printf 'Usage: %s [count]\n' "$0"
    printf 'Check the latest count epochs, newest first (default: 10).\n'
    printf 'CAR and INDEX show HTTP status: 200 = present, 404 = absent, ERR = request failed.\n'
    printf 'Terminal rows are green when both files are present, red otherwise. Set NO_COLOR=1 to disable.\n'
}

if [[ ${1:-} == --help || ${1:-} == -h ]]; then
    usage
    exit 0
fi

epoch_count=${1:-10}
if (( $# > 1 )) || [[ ! $epoch_count =~ ^[1-9][0-9]*$ ]]; then
    usage >&2
    exit 1
fi

for dependency in curl jq; do
    if ! command -v "$dependency" > /dev/null; then
        printf 'Required command not found: %s\n' "$dependency" >&2
        exit 1
    fi
done

if ! current_epoch=$(curl -fsS --max-time 20 https://api.mainnet-beta.solana.com \
    -H 'Content-Type: application/json' \
    --data '{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[]}' |
    jq -er '.result.epoch | select(type == "number" and . >= 0 and floor == .)'); then
    printf 'Failed to determine the current mainnet epoch.\n' >&2
    exit 1
fi

head_code() {
    curl -sSIL --max-time 20 -o /dev/null -w '%{http_code}' "$1"
}

green=''
red=''
reset=''
if [[ -t 1 && -z ${NO_COLOR:-} ]]; then
    green=$'\033[32m'
    red=$'\033[31m'
    reset=$'\033[0m'
fi

printf 'HTTP status: 200 = present, 404 = absent, ERR = request failed.\n' >&2
printf '%-6s %-21s %-4s %s\n' EPOCH 'SLOT RANGE (inclusive)' CAR INDEX
for ((epoch=current_epoch; epoch>=0 && current_epoch-epoch<epoch_count; epoch--)); do
    base="https://files.old-faithful.net/$epoch/epoch-$epoch"
    car=$(head_code "$base.car") || car=ERR
    index=$(head_code "$base-slot-ranges.raw") || index=ERR
    color=$red
    if [[ $car == 200 && $index == 200 ]]; then
        color=$green
    fi
    printf '%s%-6s %-21s %-4s %s%s\n' \
        "$color" "$epoch" "$((epoch*432000)):$(((epoch+1)*432000-1))" "$car" "$index" "$reset"
done
