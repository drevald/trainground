package dev.trainground.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@EnableScheduling
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}

@Component
class ChaosMonkey {
    private static final Logger log = LoggerFactory.getLogger(ChaosMonkey.class);

    @Scheduled(fixedRate = 30000)
    void maybeDie() {
//        if (ThreadLocalRandom.current().nextInt(10) == 0) {
//            log.warn("ChaosMonkey: simulating crash now");
//            System.exit(1);
//        }
    }
}
