package dev.trainground.app;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@Entity
@Table(name = "orders")
class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    private Product product;

    private BigDecimal priceAtOrderTime;

    public Order() {}

    public Long getId() { return id; }
    public Product getProduct() { return product; }
    public void setProduct(Product product) { this.product = product; }
    public BigDecimal getPriceAtOrderTime() { return priceAtOrderTime; }
    public void setPriceAtOrderTime(BigDecimal priceAtOrderTime) { this.priceAtOrderTime = priceAtOrderTime; }
}

interface OrderRepository extends JpaRepository<Order, Long> {}

@RestController
@RequestMapping("/orders")
class OrderController {
    private final OrderRepository orderRepository;
    private final ProductRepository productRepository;

    OrderController(OrderRepository orderRepository, ProductRepository productRepository) {
        this.orderRepository = orderRepository;
        this.productRepository = productRepository;
    }

    @GetMapping
    List<Order> all() {
        return orderRepository.findAll();
    }

    @PostMapping
    Order create(@RequestBody OrderRequest request) {
        Product product = productRepository.findById(request.productId())
                .orElseThrow(() -> new IllegalArgumentException("Unknown productId: " + request.productId()));
        Order order = new Order();
        order.setProduct(product);
        order.setPriceAtOrderTime(product.getPrice());
        return orderRepository.save(order);
    }
}

record OrderRequest(Long productId) {}
