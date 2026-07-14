# Build Stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /praetor-ingestion .

# Run Stage
FROM alpine:3.24

WORKDIR /

COPY --from=builder /praetor-ingestion /praetor-ingestion

RUN apk add --no-cache ca-certificates

EXPOSE 8081

USER 1000:1000

CMD ["/praetor-ingestion"]
