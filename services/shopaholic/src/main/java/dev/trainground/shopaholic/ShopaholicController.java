package dev.trainground.shopaholic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

record Product(Long id, String name, Object price) {}

@RestController
@RequestMapping("/shop")
class ShopaholicController {

    private static final Logger log = LoggerFactory.getLogger(ShopaholicController.class);

    private final WebClient webClient = WebClient.builder().build();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile List<Long> productIds = List.of();
    private Disposable subscription;

    @Value("${orders.url:http://order-service:8080/orders}")
    private String ordersUrl;

    @Value("${products.url:http://order-service:8080/products}")
    private String productsUrl;

    @PostMapping("/start")
    String start(@RequestParam(defaultValue = "5") int rps,
                 @RequestParam(defaultValue = "3") int threads,
                 @RequestParam(defaultValue = "60") int durationSeconds) {
        if (running.get()) {
            return "Already shopping";
        }
        refreshCatalog();
        if (productIds.isEmpty()) {
            return "Catalog is empty, seed products first via POST " + productsUrl;
        }

        running.set(true);
        sent.set(0);
        succeeded.set(0);
        failed.set(0);

        long intervalNanos = 1_000_000_000L / Math.max(rps, 1);

        subscription = Flux.interval(Duration.ofNanos(intervalNanos))
                .take(Duration.ofSeconds(durationSeconds))
                .flatMap(tick -> sendOrder(), threads)
                .doFinally(signal -> running.set(false))
                .subscribeOn(Schedulers.parallel())
                .subscribe();

        return "Shopaholic started (WebClient): rps=" + rps + " concurrency=" + threads
                + " duration=" + durationSeconds + "s catalogSize=" + productIds.size();
    }

    @PostMapping("/stop")
    String stop() {
        running.set(false);
        if (subscription != null) subscription.dispose();
        return "Shopaholic stopped";
    }

    @GetMapping("/status")
    String status() {
        return String.format("shopping=%s sent=%d succeeded=%d failed=%d catalogSize=%d",
                running.get(), sent.get(), succeeded.get(), failed.get(), productIds.size());
    }

    private void refreshCatalog() {
        try {
            Product[] products = webClient.get().uri(productsUrl)
                    .retrieve().bodyToMono(Product[].class).block(Duration.ofSeconds(5));
            productIds = products == null ? List.of() : List.of(
                    java.util.Arrays.stream(products).map(Product::id).toArray(Long[]::new));
        } catch (Exception e) {
            log.warn("Failed to load catalog: {}", e.getMessage());
            productIds = List.of();
        }
    }

    private Mono<Void> sendOrder() {
        List<Long> ids = productIds;
        if (ids.isEmpty()) {
            failed.incrementAndGet();
            return Mono.empty();
        }
        sent.incrementAndGet();
        Long productId = ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
        var body = Map.of("productId", productId);
        return webClient.post().uri(ordersUrl)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(String.class)
                .doOnSuccess(r -> succeeded.incrementAndGet())
                .doOnError(e -> {
                    failed.incrementAndGet();
                    log.warn("Order failed: {}", e.getMessage());
                })
                .onErrorResume(e -> Mono.empty())
                .then();
    }
}
