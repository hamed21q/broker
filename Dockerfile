FROM docker.arvancloud.ir/golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o main .

FROM docker.arvancloud.ir/alpine:latest

WORKDIR /root/

COPY --from=builder /app/main .

CMD ["./main"]
