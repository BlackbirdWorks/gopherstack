# Build stage
FROM golang:1.27.3-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder

ENV GOTOOLCHAIN=auto

WORKDIR /app

RUN apk add --no-cache nodejs npm

# Copy go mod and sum files
COPY go.mod ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build dashboard2 frontend assets before embedding in Go binary
# If the SPA was pre-built (e.g. via `task ui-build` before `task demo`), skip the npm build
# to avoid OOM inside the container. Fall back to building in-container if assets are absent.
RUN if [ ! -f dashboard/static/spa/index.html.gz ] && [ ! -f dashboard/static/spa/index.html ]; then \
        npm --prefix ui ci --include=optional && \
        NODE_OPTIONS="--max-old-space-size=8192" npm --prefix ui run build; \
    fi

# Build the Go app
RUN CGO_ENABLED=0 go build \
    -tags 'netgo osusergo static_build' \
    -trimpath \
    -buildvcs=false \
    -ldflags="-w -s -extldflags '-static -fno-PIC'" \
    -o gopherstack

# Writable state dir for the non-root runtime user (65532:65532)
FROM --platform=$BUILDPLATFORM alpine:3.22@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f AS state
RUN mkdir /data

# Final stage: distroless static (nonroot) has a world-searchable / (containerd would unpack a bare
# scratch / as 0700 root), plus ca-certificates, tzdata and a 1777 /tmp.
FROM gcr.io/distroless/static:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329

COPY --from=state --chown=65532:65532 /data /data

WORKDIR /usr/local/bin

COPY --from=builder --chown=0:0 --chmod=755 /app/gopherstack .

USER 65532:65532


# Expose port 8000 to the outside world
EXPOSE 8000

# Expose MQTT broker port
EXPOSE 1883

# Expose Azure Blob/Queue/Table (Azurite-compatible), the shared Azure
# Storage virtual-hosted listener (10010, see services/azurestoragevhost),
# Service Bus, ARM, and Cosmos DB ports
EXPOSE 10000 10001 10002 10003 10006 10010 8081

# OCI label pointing to the source repository
LABEL org.opencontainers.image.source="https://github.com/blackbirdworks/gopherstack"

# Command to run the executable
CMD ["./gopherstack"]
