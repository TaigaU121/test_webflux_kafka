package com.example.demo;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.TransactionManager;
import reactor.test.StepVerifier;
import reactor.util.concurrent.Queues;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class ReactorKafkaTest {
    // ここに、テストコードを書く！
    @Test
    public void gettingStarted() {
        String brokerConnectString = "0.0.0.0:9092";
        String topicName = "reactor-kafka-topic";

        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectString);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        SenderOptions<String, String> senderOptions =
                SenderOptions.<String, String>create(producerProperties).maxInFlight(Queues.SMALL_BUFFER_SIZE);

        KafkaSender<String, String> sender = KafkaSender.create(senderOptions);

        Flux<SenderRecord<String, String, Integer>> sendMessages =
                Flux
                        .range(1, 10)
                        .map(i -> SenderRecord.create(topicName, null, null, "key" + i, "value" + i, i));
        sender
                .send(sendMessages)
                .subscribe();

        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerConnectString);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "reactor-kafka-group");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ReceiverOptions<String, String> receiverOptions =
                ReceiverOptions
                        .<String, String>create(consumerProperties)
                        .subscription(Collections.singletonList(topicName));

        KafkaReceiver<String, String> receiver = KafkaReceiver.create(receiverOptions);

        Flux<ReceiverRecord<String, String>> receiveMessages = receiver.receive();

        Mono<List<Tuple2<String, String>>> collectedMessages =
                receiveMessages
                        .buffer(10)
                        .map(messages -> messages.stream().map(m -> Tuples.of(m.key(), m.value())).collect(Collectors.toList()))
                        .next();

        StepVerifier
                .create(collectedMessages)
                .expectNext(IntStream.rangeClosed(1, 10).mapToObj(i -> Tuples.of("key" + i, "value" + i)).collect(Collectors.toList()))
                .verifyComplete();

        sender.close();
    }
}
