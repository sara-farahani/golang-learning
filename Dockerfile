# syntax=docker/dockerfile:1

# ---- Build stage: compile a static, stripped binary ----
FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /app/gopulse ./cmd/gopulse

# ---- Runtime stage: tiny image, non-root user ----
FROM alpine:3.20
RUN adduser -D -u 10001 gopulse
COPY --from=builder /app/gopulse /usr/local/bin/gopulse
USER gopulse
EXPOSE 8080
ENTRYPOINT ["gopulse"]
