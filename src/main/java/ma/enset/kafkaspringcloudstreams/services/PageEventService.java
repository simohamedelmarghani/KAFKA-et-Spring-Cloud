package ma.enset.kafkaspringcloudstreams.services;

import ma.enset.kafkaspringcloudstreams.entities.PageEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Service
public class PageEventService {

    private static final Logger log = LoggerFactory.getLogger(PageEventService.class);
    private final Random random = new Random();

    /**
     * Service Consumer KAFKA : consomme les événements depuis le topic T1.
     */
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return input -> {
            log.info("**********************");
            log.info("PageEvent Consumer reçu : {}", input);
            log.info("**********************");
        };
    }

    /**
     * Service Supplier KAFKA : génère périodiquement des PageEvent vers le topic T2.
     */
    @Bean
    public Supplier<PageEvent> pageEventSupplier() {
        return () -> new PageEvent(
                Math.random() > 0.5 ? "P1" : "P2",
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                random.nextInt(9000)
        );
    }

    /**
     * Service de Data Analytics Real Time Stream Processing avec Kafka Streams :
     * Lit depuis T2, filtre (duration > 100), compte par nom de page sur une fenêtre de 5s,
     * stocke le résultat dans le State Store "page-count" et publie vers T4.
     */
    @Bean
    public Function<KStream<String, PageEvent>, KStream<String, Long>> kStreamFunction() {
        return input -> input
                .filter((k, v) -> v != null && v.getDuration() > 100)
                .map((k, v) -> new KeyValue<>(v.getName(), 0L))
                .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("page-count")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .map((k, v) -> new KeyValue<>(k.key(), v));
    }
}
