package com.learning.kafkaexactlyonce;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class MessageProcessing {

    @Autowired
    KafkaConsumer<String, String> consumer;

    @Autowired
    KafkaProducer<String, String> producer;

    @Value(value = "${spring.kafka.inputTopic}")
    private String inputTopic;

    @Value(value = "${spring.kafka.outTopic}")
    private String outTopic;

    @Value(value = "${spring.kafka.groupId}")
    private String groupId;

    @Autowired
    public void process() {

        producer.initTransactions();
        try {

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                log.info("records count {}", records.count());
                log.info("consumer is counting the words");
                Map<String, Integer> wordCountMap = records.records(new TopicPartition(inputTopic, 0))
                        .stream()
                        .flatMap(record -> Stream.of(record.value()
                                .split(" ")))
                        .map(word -> Pair.of(word, 1))
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue, Integer::sum));
                producer.beginTransaction();

                log.info("wordCountMap.size {} ", wordCountMap.size());
                log.info("producer is going to sleep");
                wordCountMap.forEach((key, value) -> {
                    log.info("key {}, value {} ", key, value);
                    producer.send(new ProducerRecord<>(outTopic, key, value.toString()));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });

                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                log.info("producer is computing the offsets");
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
                    long offset = partitionedRecords.get(partitionedRecords.size() - 1)
                            .offset();

                    offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
                }

                log.info("producer is sending offsets");
                producer.sendOffsetsToTransaction(offsetsToCommit, groupId);
                log.info("producer is committing");
                producer.commitTransaction();
            }
        } catch (KafkaException e) {
            producer.abortTransaction();
        }
    }
}
