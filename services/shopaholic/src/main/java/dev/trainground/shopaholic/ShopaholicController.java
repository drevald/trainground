package dev.trainground.shopaholic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
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
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

record Product(Long id, String name, Object price) {}
record Customer(Long id, String name, String city) {}

@RestController
@RequestMapping("/shop")
class ShopaholicController {

    private static final Logger log = LoggerFactory.getLogger(ShopaholicController.class);

    private final ConnectionProvider connectionProvider = ConnectionProvider.builder("shopaholic-pool")
            .maxConnections(2000)
            .pendingAcquireMaxCount(200000)
            .pendingAcquireTimeout(Duration.ofSeconds(10))
            .build();

    private final WebClient webClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(HttpClient.create(connectionProvider)))
            .build();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile List<Long> productIds = List.of();
    private volatile List<Long> customerIds = List.of();
    private Disposable subscription;

    @Value("${orders.url:http://order-service:8080/orders}")
    private String ordersUrl;

    @Value("${products.url:http://order-service:8080/products}")
    private String productsUrl;

    @Value("${customers.url:http://order-service:8080/customers}")
    private String customersUrl;

    @PostMapping("/start")
    String start(@RequestParam(defaultValue = "5") int rps,
                 @RequestParam(defaultValue = "3") int threads,
                 @RequestParam(defaultValue = "60") int durationSeconds) {
        if (running.get()) {
            return "Already shopping";
        }
        refreshCatalog();
        refreshCustomers();
        if (productIds.isEmpty() || customerIds.isEmpty()) {
            return "Catalog or customers empty, seed data first via POST " + productsUrl + " and " + customersUrl;
        }

        running.set(true);
        sent.set(0);
        succeeded.set(0);
        failed.set(0);

        subscription = Flux.range(0, Integer.MAX_VALUE)
                .take(Duration.ofSeconds(durationSeconds))
                .flatMap(tick -> sendOrder(), threads)
                .doFinally(signal -> running.set(false))
                .subscribeOn(Schedulers.parallel())
                .subscribe();

        return "Shopaholic started (WebClient): concurrency=" + threads
                + " duration=" + durationSeconds + "s catalogSize=" + productIds.size()
                + " customers=" + customerIds.size();
    }

    @PostMapping("/stop")
    String stop() {
        running.set(false);
        if (subscription != null) subscription.dispose();
        return "Shopaholic stopped";
    }

    @GetMapping("/status")
    String status() {
        return String.format("shopping=%s sent=%d succeeded=%d failed=%d catalogSize=%d customers=%d",
                running.get(), sent.get(), succeeded.get(), failed.get(), productIds.size(), customerIds.size());
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

    private void refreshCustomers() {
        try {
            Customer[] customers = webClient.get().uri(customersUrl)
                    .retrieve().bodyToMono(Customer[].class).block(Duration.ofSeconds(5));
            customerIds = customers == null ? List.of() : List.of(
                    java.util.Arrays.stream(customers).map(Customer::id).toArray(Long[]::new));
        } catch (Exception e) {
            log.warn("Failed to load customers: {}", e.getMessage());
            customerIds = List.of();
        }
    }

    private Mono<Void> sendOrder() {
        List<Long> pids = productIds;
        List<Long> cids = customerIds;
        if (pids.isEmpty() || cids.isEmpty()) {
            failed.incrementAndGet();
            return Mono.empty();
        }
        sent.incrementAndGet();
        Long productId = pids.get(ThreadLocalRandom.current().nextInt(pids.size()));
        Long customerId = cids.get(ThreadLocalRandom.current().nextInt(cids.size()));

        return webClient.get().uri(productsUrl + "/" + productId)
                .retrieve().bodyToMono(Product.class)
                .flatMap(browsedProduct -> {
                    var body = Map.of("productId", productId, "customerId", customerId);
                    return webClient.post().uri(ordersUrl)
                            .bodyValue(body)
                            .retrieve()
                            .bodyToMono(String.class);
                })
                .doOnSuccess(r -> succeeded.incrementAndGet())
                .doOnError(e -> {
                    failed.incrementAndGet();
                    log.warn("Order failed: {}", e.getMessage());
                })
                .onErrorResume(e -> Mono.empty())
                .then();
    }
}
