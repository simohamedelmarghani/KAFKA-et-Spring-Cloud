package ma.enset.kafkaspringcloudstreams.web;

import ma.enset.kafkaspringcloudstreams.entities.PageEvent;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RestController
public class PageEventRestController {

    private final StreamBridge streamBridge;
    private final InteractiveQueryService interactiveQueryService;
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public PageEventRestController(StreamBridge streamBridge, InteractiveQueryService interactiveQueryService) {
        this.streamBridge = streamBridge;
        this.interactiveQueryService = interactiveQueryService;
    }

    /**
     * Producer KAFKA via REST Controller (GET) :
     * Exemple : GET http://localhost:8080/publish/T1/P1
     */
    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable String topic, @PathVariable String name) {
        PageEvent pageEvent = new PageEvent(
                name,
                Math.random() > 0.5 ? "U1" : "U2",
                new Date(),
                random.nextInt(9000)
        );
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    /**
     * Producer KAFKA via REST Controller (POST) :
     * Permet d'envoyer un PageEvent personnalisé en JSON vers un topic.
     */
    @PostMapping("/publish/{topic}")
    public PageEvent publishCustom(@PathVariable String topic, @RequestBody PageEvent pageEvent) {
        if (pageEvent.getDate() == null) {
            pageEvent.setDate(new Date());
        }
        streamBridge.send(topic, pageEvent);
        return pageEvent;
    }

    /**
     * Endpoint SSE (Server-Sent Events) pour diffuser les résultats du Stream Data Analytics en temps réel.
     */
    @GetMapping(path = "/analytics", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter analytics() {
        SseEmitter emitter = new SseEmitter(0L);

        ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(() -> {
            try {
                Map<String, Long> stringLongMap = new HashMap<>();
                try {
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore(
                            "page-count", QueryableStoreTypes.windowStore()
                    );
                    Instant now = Instant.now();
                    Instant from = now.minusMillis(5000);
                    try (KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now)) {
                        while (fetchAll.hasNext()) {
                            KeyValue<Windowed<String>, Long> next = fetchAll.next();
                            stringLongMap.put(next.key.key(), next.value);
                        }
                    }
                } catch (Exception ignored) {
                    // Le state store peut être en cours d'initialisation au démarrage
                }
                emitter.send(SseEmitter.event().data(stringLongMap));
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
        }, 0, 1, TimeUnit.SECONDS);

        emitter.onCompletion(() -> future.cancel(true));
        emitter.onTimeout(() -> future.cancel(true));
        emitter.onError(ex -> future.cancel(true));

        return emitter;
    }
}
