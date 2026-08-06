# Multi-stage build: one image, one binary chosen at runtime by the compose
# service's command.
FROM golang:1.24-alpine AS build

WORKDIR /src

# Dependencies first so the module cache survives source-only changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO is off so the binaries run on a distroless-style base with no libc.
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/api            ./cmd/api           && \
    go build -trimpath -ldflags="-s -w" -o /out/outbox-worker  ./cmd/outbox-worker && \
    go build -trimpath -ldflags="-s -w" -o /out/webhook-worker ./cmd/webhook-worker && \
    go build -trimpath -ldflags="-s -w" -o /out/replay         ./cmd/replay        && \
    go build -trimpath -ldflags="-s -w" -o /out/migrate        ./cmd/migrate       && \
    go build -trimpath -ldflags="-s -w" -o /out/seed           ./cmd/seed          && \
    go build -trimpath -ldflags="-s -w" -o /out/example-webhook ./cmd/example-webhook

FROM alpine:3.20

# curl is used by the compose healthchecks and the demo script.
RUN apk add --no-cache ca-certificates curl && \
    adduser -D -u 10001 ledger

COPY --from=build /out/ /usr/local/bin/

USER ledger
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/api"]
