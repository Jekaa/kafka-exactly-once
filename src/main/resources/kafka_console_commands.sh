bin/kafka-console-producer.sh --broker-list localhost:9092 --topic inputTopic

bin/kafka-console-consumer.sh --topic outTopic --from-beginning --bootstrap-server localhost:9092