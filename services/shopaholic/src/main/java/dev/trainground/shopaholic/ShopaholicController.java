package dev.trainground.shopaholic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.kafka.core.KafkaTemplate;
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
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

record Product(Long id, String name, Object price) {}
record CustomerRef(Long id) {}
record CustomerPage(List<CustomerRef> content, int totalPages) {}
record OrderRequest(Long productId, Long customerId, String idempotencyKey) {}

@RestController
@RequestMapping("/shop")
class ShopaholicController {

    private static final Logger log = LoggerFactory.getLogger(ShopaholicController.class);

    private final ConnectionProvider connectionProvider = ConnectionProvider.builder("shopaholic-pool")
            .maxConnections(20000)
            .pendingAcquireMaxCount(200000)
            .pendingAcquireTimeout(Duration.ofSeconds(3600))
            .build();

    private final WebClient webClient = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(HttpClient.create(connectionProvider)))
            .build();

    private final KafkaTemplate<String, OrderRequest> kafkaTemplate;

    ShopaholicController(KafkaTemplate<String, OrderRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile List<Long> productIds = List.of();
    private volatile int customerTotalPages = 1;
    private volatile boolean useKafka = true;
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
                 @RequestParam(defaultValue = "60") int durationSeconds,
                 @RequestParam(defaultValue = "1") int browsePages,
                 @RequestParam(defaultValue = "true") boolean useKafka) {
        if (running.get()) {
            return "Already shopping";
        }
        this.useKafka = useKafka;
        refreshCatalog();
        refreshCustomerPageCount();
        if (productIds.isEmpty()) {
            return "Catalog is empty, seed products first via POST " + productsUrl;
        }

        running.set(true);
        sent.set(0);
        succeeded.set(0);
        failed.set(0);

        subscription = Flux.range(0, Integer.MAX_VALUE)
                .take(Duration.ofSeconds(durationSeconds))
                .flatMap(tick -> sendOrder(browsePages), threads)
                .doFinally(signal -> running.set(false))
                .subscribeOn(Schedulers.parallel())
                .subscribe();

        return "Shopaholic started: concurrency=" + threads
                + " duration=" + durationSeconds + "s catalogSize=" + productIds.size()
                + " customerPages=" + customerTotalPages + " browsePages=" + browsePages
                + " useKafka=" + useKafka;
    }

    @PostMapping("/stop")
    String stop() {
        running.set(false);
        if (subscription != null) subscription.dispose();
        return "Shopaholic stopped";
    }

    @GetMapping("/status")
    String status() {
        return String.format("shopping=%s sent=%d succeeded=%d failed=%d catalogSize=%d customerPages=%d useKafka=%s",
                running.get(), sent.get(), succeeded.get(), failed.get(), productIds.size(), customerTotalPages, useKafka);
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

    private void refreshCustomerPageCount() {
        try {
            CustomerPage page = webClient.get().uri(customersUrl + "?page=0&size=100")
                    .retrieve().bodyToMono(CustomerPage.class).block(Duration.ofSeconds(5));
            customerTotalPages = page == null ? 1 : Math.max(page.totalPages(), 1);
        } catch (Exception e) {
            log.warn("Failed to load customer page count: {}", e.getMessage());
            customerTotalPages = 1;
        }
    }

    private Mono<Void> sendOrder(int browsePages) {
        List<Long> pids = productIds;
        if (pids.isEmpty()) {
            failed.incrementAndGet();
            return Mono.empty();
        }
        sent.incrementAndGet();
        Long productId = pids.get(ThreadLocalRandom.current().nextInt(pids.size()));
        String idempotencyKey = UUID.randomUUID().toString();

        Mono<Void> browsing = Flux.range(0, browsePages)
                .flatMap(i -> {
                    Long randomProductId = pids.get(ThreadLocalRandom.current().nextInt(pids.size()));
                    return webClient.get().uri(productsUrl + "/" + randomProductId)
                            .retrieve().bodyToMono(Product.class)
                            .onErrorResume(e -> Mono.empty());
                }, 5)
                .then();

        Mono<Long> customerIdMono;
        if (browsePages > 0) {
            int randomPage = ThreadLocalRandom.current().nextInt(customerTotalPages);
            customerIdMono = webClient.get().uri(customersUrl + "?page=" + randomPage + "&size=100")
                    .retrieve().bodyToMono(CustomerPage.class)
                    .flatMap(customerPage -> {
                        if (customerPage.content().isEmpty()) {
                            return Mono.error(new IllegalStateException("Empty customer page"));
                        }
                        return Mono.just(customerPage.content()
                                .get(ThreadLocalRandom.current().nextInt(customerPage.content().size()))
                                .id());
                    });
        } else {
            customerIdMono = Mono.just((long) (ThreadLocalRandom.current().nextInt(100000) + 1));
        }

        boolean kafkaPath = useKafka;

        return browsing.then(customerIdMono)
                .flatMap(customerId -> {
                    OrderRequest request = new OrderRequest(productId, customerId, idempotencyKey);
                    if (kafkaPath) {
                        return Mono.fromFuture(
                                kafkaTemplate.send("order-requests", customerId.toString(), request)
                                        .thenApply(result -> "sent")
                        );
                    } else {
                        return webClient.post().uri(ordersUrl)
                                .bodyValue(request)
                                .retrieve()
                                .bodyToMono(String.class)
                                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                                    .maxBackoff(Duration.ofSeconds(2)));
                    }
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
