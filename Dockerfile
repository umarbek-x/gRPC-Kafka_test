# syntax=docker/dockerfile:1
# One Dockerfile for all services: docker build --build-arg SERVICE=order-service .
#
# Multi-stage build: the first stage has the whole Go toolchain (~1 GB); the
# final image contains only our static binary (~20 MB). Smaller images start
# faster and have a much smaller attack surface.

ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

# Copy go.mod/go.sum first: Docker caches this layer, so dependencies are
# re-downloaded only when they change, not on every code edit.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG SERVICE
# CGO_ENABLED=0  -> fully static binary (no libc needed at runtime)
# -trimpath      -> reproducible builds, no local paths in the binary
# -ldflags -s -w -> strip debug symbols (smaller binary)
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

# Distroless: no shell, no package manager, runs as a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
