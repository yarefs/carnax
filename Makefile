.PHONY: broker cli all

cli:
	go build ./cmd/carnax-cli

broker:
	go build ./cmd/carnax-broker

all: cli broker