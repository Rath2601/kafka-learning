# Apache Kafka Commands (Local Setup on Windows)

## Can be configured in CLI (with less Java configuration)

```bash
1. Start Zookeeper      -> .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
2. Start Kafka Server   -> .\bin\windows\kafka-server-start.bat .\config\server.properties
3. List Existing Topics -> .\bin\windows\kafka-topics.bat --bootstrap-server <URL> --list
4. Create a Topic       -> .\bin\windows\kafka-topics.bat --bootstrap-server <URL> --create --replication-factor 3 --partitions 4 --topic <topic-name>
5. Consume from a Particular Offset and Partition -> .\bin\windows\kafka-console-consumer.bat \
  --bootstrap-server localhost:9092 \
  --topic first-topic \
  --partition 2 \
  --from-beginning \
  --offset earliest \
  --timeout-ms 20000 \
  --key-deserializer StringDeserializer \
  --value-deserializer JsonDeserializer
6. Alter Partition Count -> .\bin\windows\kafka-topics.bat --bootstrap-server \<URL> --alter --topic \<topic-name> --partitions \<no-of-partitions>
7. To delete a topic     -> .\bin\windows\kafka-topics.bat --bootstrap-server \<URL> --delete --topic \<topic-name>
8. Run a Producer        -> .\bin\windows\kafka-console-producer.bat --topic <topic-name> --broker-list <URL>
9. info on consumergroup -> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server <URL> --describe --group <group-name>
10.delete a consumergroup -> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server <URL> --delete --group my-group --group <group-name>
11.reset a offset        -> .\bin\windows\kafka-consumer-groups.bat --bootstrap-server <URL> --reset-offsets --group <group-name> --topic <topic-name> --to-earliest
```
## For multiple brokers 

If we need multiple brokers to be configured we can have **multiple brokers properties source file** and then start each server with **its own properties**.
```bash
1. create a topic  -> kafka-topics.bat --bootstrap-server localhost:9091,localhost:9092,localhost:9093 --create --topic <topic-name> --replication-factor 3 --partitions 10
2. produce a message -> kafka-console-producer.bat --bootstrap-server localhost:9091,localhost:9092,localhost:9093 --topic <topic-name>
3. consume a message -> kafka-console-consumer.bat --bootstrap-server localhost:9091,localhost:9092,localhost:9093 --topic nish-topic --from-beginning
```

## Producer and Consumer config 

### Producer Config
```java
@Configuration
public class KafkaProducerConfig {
	
	  
   @Value(value = "${spring.kafka.bootstrap-servers}")
   private String bootstrapAddress;

	@Bean
	public ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	@Primary
	public KafkaTemplate<String, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
}
```

### Consumer Config


