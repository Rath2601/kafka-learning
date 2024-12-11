# kafka-learning
## Messaging queue (referred to as point to point)
A message queue is a component of messaging middleware solutions that enables independent applications and services to exchange information.

**note in MQ**:
* **A message** is delivered to **only one receiver** (1:1 relationship)
* **Messages in a queue are retained** even if there are **no active receivers**.
* **Multiple receivers* can compete for messages, **enabling load balancing**. similarly there can be **multiple producers** can also send data into MQ.
* Messages are placed in the queue in the order they are produced (FIFO).
  The **order of consumption** depends on factors like:
  * Message expiration
  * Priority
  * Selectors
  * Consumer's processing rate
    
**uses of messaging queue**:
1. Asynchronous feature (decreases latency by reducing response time)
2. Retry capability
3. pace matching (message per second would different in producer and consumer , the messaging queue will act as a buffer)

### pub-sub (created after messaging) 

## Kafka internal
### Kafka Architecture - Key Characteristics

#### Consumer Groups and Partition Assignment
1. **Same Group ID**:
   - Consumers within the same group share partitions.
   - Example (Topic with 12 partitions):
     - **2 Consumers** → Each reads 6 partitions (different sets of messages).
     - **4 Consumers** → Each reads 3 partitions.

2. **Different Group IDs**:
   - Consumers with unique group IDs read **all partitions independently**.
   - Example (Topic with 12 partitions):
     - **2 Consumers (different group IDs)** → Both read all 12 partitions.
     - **4 Consumers (different group IDs)** → Each reads all partitions independently.

3. **Consumer per Partition**:
   - A **single consumer can consume** messages from **multiple topics** simultaneously.
   - Number of consumers in a group ≤ Number of partitions.
   - Example:
     - Topic A (2 partitions), Topic B (3 partitions) = Total 5 partitions:
       - **1 Consumer** → Reads all 5 partitions.
       - **2 Consumers** → Kafka distributes 2-3 partitions among them.
       - **5 Consumers** → Each gets 1 partition.
         
#### Message Format
- Messages include:
  - **Key**: Optional, used for partitioning. (**producer publishing logic**)
     * Kafka hashes the key to determine the partition.
     * Kafka assigns partitions in a round-robin manner unless a specific partition is defined.
  - **Value**: The actual data/message.
  - **Partition**: Specifies the partition (optional).
  - **Topic**: The topic name.

#### Kafka Consumer Groups and Broadcasting
1. **Independent Consumption**:
   - Multiple consumer groups can consume the same topic independently without interference.
2. **Broadcasting**:
   - Enables delivery of the same messages to different systems (e.g., logging, analytics, notifications) by using different consumer groups.
3. **Offset Tracking**:
   - Each group maintains its own offsets, ensuring isolated and independent message processing.

#### Consumer Offset Storage (Pre-0.8.1.1)
* **Old Method**: Offsets were stored in ZooKeeper.
* **Problem**: ZooKeeper struggles with scalability, especially for write-heavy operations.
* **Challenge**: High number of offsets due to consumer-count * partition-count.
#### Kafka Offset Storage (Post-0.8.1.1)
* **New Method**: Consumers commit offsets in Kafka itself.
* Offsets are written to a durable, replicated, and highly available topic.
* **Durability & Availability**: Ensures that offset data is highly available and resilient.



