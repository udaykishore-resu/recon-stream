# syntax=docker/dockerfile:1.7
FROM golang:1.26-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/recon-stream ./cmd/recon-stream

FROM gcr.io/distroless/static-debian12:nonroot
LABEL org.opencontainers.image.title="recon-stream" \
      org.opencontainers.image.description="Streaming reconciliation engine for ledgers, payment rails and card processors" \
      org.opencontainers.image.source="https://github.com/udaykishore-resu/recon-stream" \
      org.opencontainers.image.licenses="Apache-2.0"
COPY --from=build /out/recon-stream /recon-stream
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/recon-stream"]
