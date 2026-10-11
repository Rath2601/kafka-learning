#### Asynchronous Messaging
##### Working — brief points
###### Message Queue (point-to-point)
- Producer → queue → **one** consumer gets each message
- Message **deleted** after it's acknowledged
- Multiple consumers **compete** → load balancing
- Retained until consumed; FIFO-ish; supports retry + DLQ
###### Pub-Sub
- Producer → topic → **copy to every** subscriber (1:many)
- **Transient** — miss it if you weren't subscribed; no replay
- Publisher doesn't know/care who's listening → fan-out
###### Event Streaming (pub-sub + durable log)
- Producer → append-only **log** (partitioned)
- Consumers read by **offset**, at their own pace, can **replay/rewind**
- Retained for days/forever; many consumer groups read independently; high throughput
###### Tools & use cases
| Family | Tools / Services | Use Case |
|---|---|---|
| **Message Queue** | RabbitMQ, Amazon SQS | Background jobs / task processing — one worker handles each item (e.g. process each order once) |
| **Pub-Sub** | Amazon SNS, Google Cloud Pub/Sub | Fan-out one event to many services at once (e.g. "order placed" → inventory + email + analytics) |
| **Event Streaming** | Apache Kafka, Confluent (managed Kafka), Amazon Kinesis | Replayable event backbone / real-time analytics (e.g. transactions consumed by fraud + ledger + analytics, replayable) |

Kafka deep dive : 
