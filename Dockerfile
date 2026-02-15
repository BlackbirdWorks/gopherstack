# Build stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Build the Go app
RUN go build \
    -tags 'netgo osusergo static_build' \
    -trimpath \
    -ldflags="-w -s -extldflags '-static -fno-PIC'" \
    -o gopherstack

# Final stage
FROM scratch

WORKDIR /root/

# Copy the Pre-built binary from the previous stage
COPY --from=builder /app/gopherstack .

# Expose port 8000 to the outside world
EXPOSE 8000

# Command to run the executable
CMD ["./gopherstack"]
