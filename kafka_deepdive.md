#### Apache Kafka - Definition
**high-throughput, low-latency distributed event streaming platform** — an append-only, partitioned, durable log for real-time data feeds. Producers write events, consumers read them at their own pace, and events are retained so they can be **replayed**.
- Where it sits: **Event Streaming = Pub-Sub + durable log** (a superset of classic pub-sub; can also mimic a point-to-point queue via consumer groups).
---
##### When & why to use Kafka (use cases)
- **Distributed logging** — aggregate logs/events from many microservices into one stream.
- **Stream processing (high-volume, real-time)** — process huge event streams live (e.g. trade-app transactions, ad-click aggregator).
- **Data integration** — a central hub stitching records from many sources (e.g. hospital EHR + lab results + insurance claims).
- **Pub/Sub — multiple consumers, same stream** — fan out to many independent consumer groups, each reading at its own offset (e.g. a tweet notifies all followers).
- **As a message queue** — consumer groups give queue-style "one worker per message" semantics (e.g. YouTube transcoding jobs).
- **Ordered processing / buffering** — absorb bursts and process in order (e.g. Ticketmaster waiting queue).
- **Decouple producer & consumer** — each side scales independently; the durable log sits in between.

**The unifying reason:** you need **durability + replay + high throughput + multiple independent readers** all at once — the combination only streaming gives, and Kafka is the standard for it.
##### When NOT to reach for Kafka (quick guardrail)

- Simple background job queue with complex routing → RabbitMQ/SQS is simpler.
- Tiny scale / low volume → Kafka's operational weight isn't worth it.
- Need request-reply / RPC → that's not Kafka's model.
---
#### Architecture
##### Hierarchy
**Cluster → Brokers (servers) → Topic (logical grouping) → Partitions (physical append-only logs, distributed across brokers) → Offset → Record**
- **Broker** — a server; holds **many partitions from many topics** (not one queue).
- **Partition** — a *queue*: an ordered, immutable, append-only log (like a log file). The unit of scaling and parallelism.
- **Topic** — a logical grouping of partitions, used to organize data. A topic's partitions are spread **across** brokers.
- **Offset** — a per-partition sequential ID; tracks each message in order and tracks consumption progress.
- **Record** — the message / event.
##### What each piece solves
- **Many brokers** → handle too-many-events / scale-out.
- **Partitioning by key** → solves ordering (without it, the same user's events could be processed out of order, e.g. *payment processed before order created*).
- **Consumer groups** → solve "too many events to consume" by parallelizing across consumers.
##### Ordering model (burn this in)
- Offset is **per-partition**, not global.
- **Ordering is guaranteed only within a partition**, never across the whole topic.
- Same **key → same partition → ordered** for that key.

##### Consumer group rules
- From a partition, only **one consumer** can consume within a group.
- One consumer can read **one or more partitions** if `consumers < partitions`.
- **Ideal: `consumers == partitions`** (each partition served by exactly one consumer).
- If **`consumers > partitions`**, the **extra consumers sit idle** (a partition is never split across two consumers in the same group). So max useful parallelism in a group = partition count.
- **Different groups → independent copies** of the same partition's data.
- Each group maintains its **own separate offset** (reading progress).

##### Producer flow
1. **Find partition:**
   - With a **key** → `partition = murmur2(key) % numPartitions`.
   - With **no key** → **sticky partitioner** (fills one partition's batch, then rotates); spreads load, no cross-partition ordering.
2. **Find the partition's leader replica** (producers/consumers always talk to the **leader**; followers only replicate).
3. **Leader appends** the record, assigns the **offset**, followers replicate.

##### Consumer flow
1. **Poll** (consumers pull; brokers never push) and read from the current **offset**.
2. Message delivered to consumer → process it.
3. **Commit offset** — stored in the internal topic **`__consumer_offsets`**.
   - If a consumer goes down, it resumes from the **last committed offset**.
   - Commit can be **auto** (`enable.auto.commit`, periodic) or **manual**. Commit **timing** decides delivery semantics (below).
---
##### Configuration
###### Scalability
- **Keep messages small — aim for `<1MB`** (default `max.message.bytes` ≈ 1MB) for performance.
  - *Pattern:* don't put a whole video/blob in the message; put an **S3 URL** and let the consumer (e.g. transcoder) do the heavy lifting.
- **Per-broker "1TB / 10k msgs/sec" are loose rules of thumb, not hard limits** — real capacity is hardware-bound and often far higher with batching + compression. The real lever is **partition count** for parallelism.
- **Choose a good partitioning key** — a bad key → **hot partition** (skewed load).
- **No ordering needed?** → use no key (spread across all partitions).
- **Partitions can only be increased, never decreased** — and increasing them **breaks the key→partition mapping** for existing keys, so plan with room to grow.

###### Fault tolerance
- **`acks=all`** — the producer waits until all **in-sync replicas (ISR)** acknowledge before the write is considered successful (durability over performance).
- **Pair with `min.insync.replicas`** to make `acks=all` meaningful. Classic safe setup: **`RF=3, min.insync.replicas=2, acks=all`**. (Without `min.insync.replicas`, if the ISR shrinks to just the leader, `acks=all` can still lose data — most common durability gap.)
- **Replication factor** — how many copies of a partition exist (e.g. `3` = 1 leader + 2 followers).
- **Rebalancing** — if a consumer in a group goes down (or joins), partitions are reassigned. Newer **cooperative/incremental rebalancing** avoids the old stop-the-world pause.
- **Commit timing matters** — determines whether you lose or duplicate messages on failure.

**Retry & DLQ**
- **Not built into core Kafka** for regular consumers. Configure via a library:
  - **Kafka Connect** — native DLQ since Kafka 2.0 (`errors.deadletterqueue.topic.name`).
  - **Kafka Streams** — has its own DLQ support.
  - **Spring Kafka** — `@RetryableTopic`, `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`; dead-letter topic conventionally named original + `.DLT`.

**Producer retries — native and on by default**
- Since **Kafka 3.0**, the **idempotent producer is enabled by default**, which automatically sets **`retries=Integer.MAX_VALUE`** and **`acks=all`**.
- Idempotence → the same message retried results in **exactly one copy** written (no duplicates).
- Total retry window bounded by **`delivery.timeout.ms`** (default 2 min).

###### Optimization
- **Batch messages** in the producer (`batch.size`, `linger.ms`).
- **Compress messages** in the producer (`compression.type`: gzip / snappy / lz4 / zstd).
- They **compound** — bigger batches compress better, so tune both together.
---
##### Other important concepts
###### Delivery semantics
- **At-most-once** — commit offset **before** processing → possible loss.
- **At-least-once** — commit **after** processing → possible duplicates (common default).
- **Exactly-once (EOS)** — idempotent producer + transactions.
###### Retention & log compaction
- Messages aren't kept forever by default. Retention is **time-based** (`retention.ms`) or **size-based** (`retention.bytes`).
- **Log compaction** — alternative that keeps only the **latest value per key** (great for changelog/state topics).
###### KRaft (replaced ZooKeeper)
- KRaft was production-ready in **Kafka 3.3**; **Kafka 4.0 removed ZooKeeper entirely** — KRaft is now the **only** metadata management mode.
- Metadata lives in an internal topic **`__cluster_metadata`**; brokers consume it directly (faster propagation than polling ZooKeeper).
- On modern Kafka, **there is no ZooKeeper** — ignore older tutorials that assume it.
###### Pull model
- Consumers **poll** the broker; brokers never push. This is what lets each consumer read at its own pace.
###### Share Groups (native queue semantics)
- **Kafka 4.0** added **Queues for Kafka via Share Groups** — multiple consumers can **share a single partition** (classic competing-consumers / queue style), which the old one-consumer-per-partition rule prevented.
---
