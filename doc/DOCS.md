# carnax
This document is the centralised location of all information relating to carnax. 
In the future, I hope for this document to serve as a polished overview of carnax and the internal workings. For the time being, however, this
document acts as a centralised place for dumping knowledge, ideas, and some guidelines or documentation on carnax and how it works.

## Design Choices
Raft is used to maintain consensus on segment states across the nodes.

## log
The following list is a rough set of rules that apply to the carnax `log`.

1. Any node can write to the active segment
   1. an ACK is only returned once the message is committed into the object-store.
2. Only the leader flushes segments to the object-store
   1. TBD
3. Writing to the log
   1. lock the shared state
   2. reserve an offset
   3. write into the segment for the topic-partition
   4commit offset as latest
   5unlock shared state
4. Flushing a segment
   1. lock the shared state AND make a new segment for concurrent/incoming writes
   2. compress into a final segment (with indexes)
   3. flush to object-store
   4. clear the state
   5. unlock the shared state
5. Consuming messages
   1. subscribe to a topic + partition(s)
   2. the segment gets loaded into memory
   3. subsequent reads can be routed to the node that has this segment to avoid duplicate loads

### problems
These are weak areas that need to be hashed out further.

#### segments need to be re-ordered before being stored in the object-store
option 1: we maintain a shared view of the segment and order on write.
option 2: we re-process before committing to the object-store (just-in-time)
option 3: we store order metadata and order on consume

#### should we allow followers to publish segments?
If you have 40 partitions over 40 instances and only 1 leader this is a lot of work to flush to an object-store.

## Appendix

### Resources
These are some resources that have been useful and/or references in the development of carnax.

- [Kafka Segment Retention](https://strimzi.io/blog/2021/12/17/kafka-segment-retention/)
- [Rebalancing](https://chrzaszcz.dev/2019/06/kafka-rebalancing/)
- [Consumer Poll Behaviour](https://stackoverflow.com/questions/37943372/kafka-consumer-poll-behaviour)
