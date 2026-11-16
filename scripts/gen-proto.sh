#!/usr/bin/env bash
# Regenerates Go code from .proto files.
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$(go env GOPATH)/bin"
rm -rf gen/go && mkdir -p gen/go
protoc -I api/proto \
  --go_out=gen/go --go_opt=paths=source_relative \
  --go-grpc_out=gen/go --go-grpc_opt=paths=source_relative \
  order/v1/order.proto events/v1/events.proto
echo "generated into gen/go"
