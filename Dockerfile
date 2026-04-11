ARG REGISTRY=docker.io

###### BUILD CONTAINER ######
FROM $REGISTRY/golang:latest AS builder

ENV PATH=$PATH:$GOPATH/bin

RUN mkdir -p /go/src/tx
COPY go.mod go.sum /go/src/tx/
COPY server /go/src/tx/server

WORKDIR /go/src/tx
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -tags netgo -ldflags='-s -w -extldflags "-static"' -o tx ./server

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.4/dumb-init_1.2.4_x86_64
RUN chmod +x /usr/local/bin/dumb-init

###### RUNTIME CONTAINER   ######
FROM alpine:latest

COPY --from=builder /usr/local/bin/dumb-init /usr/bin/dumb-init
COPY --from=builder /go/src/tx/tx /usr/local/bin/tx

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/tx", "filesrv"]
