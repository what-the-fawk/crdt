FROM golang:latest

WORKDIR /app

COPY . ./

RUN go build -o server .

ENTRYPOINT ["./server"]
