package dev.trainground.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class);
    }
}

@Component
class ChaosMonkey {
    @Scheduled(fixedRate = 30000)
    void maybeDie() {
        if (ThreadLocalRandom.current().nextInt(10) == 0) {
            System.out.println("ChaosMonkeyL simulating crash now");
            System.exit(1);
        }
    }
}
