# carnax-cli
This is the command line interface (client) for talking to a carnax broker. carnax-cli is an 
interactive CLI/REPL that you can run commands on which are sent to the carnax broker using an underlying grpc client. 

## building

```bash
$ go build ./cmd/carnax-cli
$ ./carnax-cli
```