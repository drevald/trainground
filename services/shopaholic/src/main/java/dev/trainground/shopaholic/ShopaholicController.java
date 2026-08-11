package dev.trainground.shopaholic;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@RestController
@RequestMapping("/shop")
class ShopaholicController {

    private final RestTemplate restTemplate = new RestTemplate();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private ExecutorService executor;

    @Value("@{target.url:http://app:8080/orders}")
    private String targetUrl;

    @PostMapping("/start")
    String start(@RequestParam(defaultValue = "5") int rps,
                 @RequestParam(defaultValue = "3") int threads,
                 @RequestParam(defaultValue = "60") int durationSeconds) {
        if (running.get()) {
            return "Already shopping";
        }
        running.set(true);
        sent.set(0);
        succeeded.set(0);
        failed.set(0);

        executor = Executors.newFixedThreadPool(threads);
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
        return "Shopaholic started: rps=" + rps + " threads=" + threads + " duration=" + durationSeconds + "s";
    }

    @GetMapping("/status")
    String status() {
        return String.format("shopping=%s sent=%d succeeded=%d failed=%d",
                running.get(), sent.get(), succeeded.get(), failed.get());
    }

    private void sendOrder() {
        sent.incrementAndGet();
        try {
            var body = Map.of("item", "item-" + ThreadLocalRandom.current().nextInt(10000));
            restTemplate.postForObject(targetUrl, body, String.class);
            succeeded.incrementAndGet();
        } catch (Exception e) {
            failed.incrementAndGet();
        }
    }

}
