FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /smart-scheduler ./cmd/controller

FROM alpine:3.18
RUN apk add --no-cache iputils ca-certificates
COPY --from=builder /smart-scheduler /smart-scheduler
ENTRYPOINT ["/smart-scheduler"]