# `carnax`

carnax is a "distributed" object-store based write ahead log. It uses Raft to maintain consensus and provide a CP
system.

## Summary

carnax aims to be:

* A kafka-like distributed write ahead log
* Built on top of S3 for those sweet juicy guarantees of durability and availability
* Simple and easy to read (the code), improve (the code), and understand (the software)

What you can hopefully use it for:

* Brokering messages to/from places
* Persistent data storage in log-form

Caveats:

* Whilst Carnax is heavily inspired by Kafka it is not the same and does not aim to be a slot-in replacement.
* Carnax is still trying to achieve a 'product-market fit'; things are subject to change.

## Build

### Pre-requisites

You will need the following to build carnax from source:

1. go 1.23
2. buf 1.42
3. localstack 3.7.2
4. raftadmin

### Building

```bash
# first we must generate the proto go files
# these are generated into proto/
$ brew install buf
$ buf generate .

# run the go build
$ go --version # 1.23
$ go build ./cmd/carnax-broker
$ go build ./cmd/carnax-cli

# localstack 3.7.2 is used
# this is to make an s3 store (not used yet)
$ brew install localstack/tap/localstack-cli
$ localstack start -d
$ pip3 install awslocal
$ export AWS_REGION=us-east-1
$ awslocal s3api create-bucket --bucket carnax-test-bucket

$ ./cmd
```

### Running

```bash
# assuming you have run the steps in the build section
$ ./carnax
```

## License

Licensed under [Apache License 2.0](/LICENSE)