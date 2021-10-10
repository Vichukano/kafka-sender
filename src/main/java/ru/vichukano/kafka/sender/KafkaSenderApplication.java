package ru.vichukano.kafka.sender;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderRecord;

import java.util.UUID;
import java.util.function.Function;

@Slf4j
public class KafkaSenderApplication {
    private static final String TOPIC = "my-example-topic";

    public static void main(String[] args) {
        var sender = new ReactiveKafkaSender().kafkaSender();
        Flux<SenderRecord<String, String, String>> outboundFlux =
                Flux.range(1, 10)
                        .map(String::valueOf)
                        .map(i -> SenderRecord.create(TOPIC, null, null, UUID.randomUUID().toString(), i, i));
        sender.send(outboundFlux)
                .doOnError(e -> log.error("Send failed", e))
                .doOnNext(r -> log.info("Successfully send: {} - {}", r.correlationMetadata(), r.recordMetadata()))
                .subscribe();
    }
}
