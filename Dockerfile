# Distributed Event Pipeline - Dockerfile

# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o consumer ./cmd/consumer
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o producer ./cmd/producer
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o dead-letter ./cmd/dead-letter

# Runtime stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binaries
COPY --from=builder /app/consumer .
COPY --from=builder /app/producer .
COPY --from=builder /app/dead-letter .

# Copy config
COPY configs/config.yaml ./configs/

# Create non-root user
RUN adduser -D -u 1000 appuser && \
    chown -R appuser:appuser /app

USER appuser

# Expose ports
EXPOSE 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/health || exit 1

# Entry point
ENTRYPOINT ["./consumer"]