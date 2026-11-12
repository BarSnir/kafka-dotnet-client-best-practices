#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
exec sh scripts/run-stage.sh high-throughput
