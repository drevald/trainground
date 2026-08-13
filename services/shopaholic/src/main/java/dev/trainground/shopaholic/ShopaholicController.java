package dev.trainground.shopaholic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

record Product(Long id, String name, Object price) {}

@RestController
@RequestMapping("/shop")
class ShopaholicController {

    private static final Logger log = LoggerFactory.getLogger(ShopaholicController.class);

    private final RestTemplate restTemplate = new RestTemplate();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private ExecutorService executor;
    private volatile List<Long> productIds = List.of();

    @Value("${orders.url:http://app:8080/orders}")
    private String ordersUrl;

    @Value("${products.url:http://app:8080/products}")
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

        executor = Executors.newVirtualThreadPerTaskExecutor();
        long delayMs = 1000L * threads / Math.max(rps, 1);
        long endAt = System.currentTimeMillis() + durationSeconds * 1000L;

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                while (running.get() && System.currentTimeMillis() < endAt) {
                    sendOrder();
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                running.set(false);
            });
        }
        return "Shopaholic started: rps=" + rps + " threads=" + threads
                + " duration=" + durationSeconds + "s catalogSize=" + productIds.size();
    }

    @PostMapping("/stop")
    String stop() {
        running.set(false);
        if (executor != null) executor.shutdownNow();
        return "Shopaholic stopped";
    }

    @GetMapping("/status")
    String status() {
        return String.format("shopping=%s sent=%d succeeded=%d failed=%d catalogSize=%d",
                running.get(), sent.get(), succeeded.get(), failed.get(), productIds.size());
    }

    private void refreshCatalog() {
        try {
            Product[] products = restTemplate.getForObject(productsUrl, Product[].class);
            productIds = products == null ? List.of() : List.of(
                    java.util.Arrays.stream(products).map(Product::id).toArray(Long[]::new));
        } catch (Exception e) {
            log.warn("Failed to load catalog: {}", e.getMessage());
            productIds = List.of();
        }
    }

    private void sendOrder() {
        sent.incrementAndGet();
        try {
            List<Long> ids = productIds;
            if (ids.isEmpty()) {
                failed.incrementAndGet();
                return;
            }
            Long productId = ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
            var body = Map.of("productId", productId);
            restTemplate.postForObject(ordersUrl, body, String.class);
            succeeded.incrementAndGet();
        } catch (Exception e) {
            failed.incrementAndGet();
            log.warn("Order failed: {}", e.getMessage());
        }
    }
}
