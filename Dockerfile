# Build Stage — compile on the native CI runner instead of emulating the target.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /praetor-ingestion .

# Run Stage
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

WORKDIR /

COPY --from=builder /praetor-ingestion /praetor-ingestion

RUN apk add --no-cache ca-certificates

EXPOSE 8081

USER 1000:1000

CMD ["/praetor-ingestion"]
