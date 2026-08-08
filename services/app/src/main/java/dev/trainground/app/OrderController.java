package dev.trainground.app;

import java.util.List;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Entity
class Order {

    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    private Long id;
    private String item;

    public Order() {}
    public Order(String item) {
        this.item = item;
    }

    public Long getId() {
        return id;
    }

    public String getItem() {
        return item;
    }

}

interface OrderRepository extends JpaRepository<Order, Long> {}

@RestController
@RequestMapping("/orders")
class OrderController {
    
    private final OrderRepository repository;

    OrderController(OrderRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    List<Order> all() {
        return repository.findAll();
    }

    @PostMapping
    Order create(@RequestBody Order order) {
        return repository.save(order);
    }

}
