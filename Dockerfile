FROM golang:latest

# otherwise we start in the $GOPATH
WORKDIR /app

COPY . .

RUN go mod tidy

RUN go get ./...

RUN make broker

CMD [ "/app/carnax-broker", "--raft_bootstrap", "--raft_id=nodeA" ]