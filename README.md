# Carnax
Carnax is a "distributed" object-store based commit log. It uses Raft to maintain consensus and provide a CP system.

## Summary
Carnax aims to be:

* A Kafka-inspired distributed commit log
* Built on top of S3 for those sweet juicy guarantees of durability and availability
* Simple and easy to read (the code), improve (the code), and understand/operate (the software)

What you can hopefully use it for:

* Brokering messages to/from places
* Persistent data storage in log-form

Foundational caveats:

* Whilst Carnax is heavily inspired by Kafka, it is not the same, and does not aim to be a slot-in replacement. The log structure, API, and configuration are fairly similar to Kafka however compatibility is not guaranteed for the long term.
* Due to the usage of object-stores, the 'ack' patterns, as well as the balanced optimisation against lower costs & efficiency Carnax has some important trade-offs around latency. In other words, sub-millisecond message throughput is a strong anti-pattern in usage. Seek alternatives such as Redis or Kafka.

**Short-term*** caveats:

* Carnax is still trying to achieve a 'product-market fit'; things are subject to change.
* The current design is _not yet_ built around long retention periods, i.e. time periods longer than a few weeks or even days of data.
  * Although a lot of the index lookups are not currently optimised for this, in the long-term carnax should be fit for long-term data fetching and storage.
* There are some bottlenecks around the usage of Raft for consensus
  * It might be for now that, if running carnax, a 'cluster'-per-topic approach would be beneficial to avoid this.

> 💡 In the future as the design matures these caveats can be addressed and optimised for more general use-cases.

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