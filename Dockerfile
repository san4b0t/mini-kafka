FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
# RUN go mod download (if there were dependencies)

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /mini-kafka ./cmd/broker

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /mini-kafka .

EXPOSE 8080
ENTRYPOINT ["./mini-kafka"]