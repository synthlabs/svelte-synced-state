#!/usr/bin/env sh
set -eu

go test ./...
pnpm --dir ui test
pnpm --dir ui check
pnpm --dir ui build
