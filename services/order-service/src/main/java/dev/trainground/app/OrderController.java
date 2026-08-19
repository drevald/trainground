package dev.trainground.app;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.kafka.annotation.KafkaListener;

@Entity
@Table(name = "orders", indexes = {
    @Index(name = "idx_orders_idempotency_key", columnList = "idempotency_key")
})
class Order {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    private Product product;

    @ManyToOne
    private Customer customer;

    @ManyToOne
    private Warehouse warehouse;

    private BigDecimal priceAtOrderTime;

    private String idempotencyKey;

    public Order() {}

    public Long getId() { return id; }
    public Product getProduct() { return product; }
    public void setProduct(Product product) { this.product = product; }
    public Customer getCustomer() { return customer; }
    public void setCustomer(Customer customer) { this.customer = customer; }
    public Warehouse getWarehouse() { return warehouse; }
    public void setWarehouse(Warehouse warehouse) { this.warehouse = warehouse; }
    public BigDecimal getPriceAtOrderTime() { return priceAtOrderTime; }
    public void setPriceAtOrderTime(BigDecimal priceAtOrderTime) { this.priceAtOrderTime = priceAtOrderTime; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(String idempotencyKey) { this.idempotencyKey = idempotencyKey; }
}

interface OrderRepository extends JpaRepository<Order, Long> {
    Optional<Order> findByIdempotencyKey(String idempotencyKey);
}

@RestController
@RequestMapping("/orders")
class OrderController {
    private final OrderRepository orderRepository;
    private final ProductRepository productRepository;
    private final CustomerRepository customerRepository;
    private final InventoryRepository inventoryRepository;

    OrderController(OrderRepository orderRepository,
                     ProductRepository productRepository,
                     CustomerRepository customerRepository,
                     InventoryRepository inventoryRepository) {
        this.orderRepository = orderRepository;
        this.productRepository = productRepository;
        this.customerRepository = customerRepository;
        this.inventoryRepository = inventoryRepository;
    }

    @GetMapping
    List<Order> all() {
        return orderRepository.findAll();
    }

    @KafkaListener(topics = "order-requests", groupId = "order-service-group", concurrency = "3")
    void consumeOrderRequest(OrderRequest request) {
        processOrder(request);
    }

    @PostMapping
    Order create(@RequestBody OrderRequest request) {
        return processOrder(request);
    }

    private Order processOrder(OrderRequest request) {
        if (request.idempotencyKey() != null) {
            Optional<Order> existing = orderRepository.findByIdempotencyKey(request.idempotencyKey());
            if (existing.isPresent()) {
                return existing.get();
            }
        }

        Product product = productRepository.findById(request.productId())
                .orElseThrow(() -> new IllegalArgumentException("Unknown productId: " + request.productId()));
        Customer customer = customerRepository.findById(request.customerId())
                .orElseThrow(() -> new IllegalArgumentException("Unknown customerId: " + request.customerId()));

        Warehouse warehouse = findNearestWarehouseWithStock(product, customer.getCity());

        Order order = new Order();
        order.setProduct(product);
        order.setCustomer(customer);
        order.setWarehouse(warehouse);
        order.setPriceAtOrderTime(product.getPrice());
        order.setIdempotencyKey(request.idempotencyKey());
        return orderRepository.save(order);
    }

    private Warehouse findNearestWarehouseWithStock(Product product, String customerCity) {
        List<Inventory> stockEntries = inventoryRepository.findByProduct(product);

        Optional<Inventory> sameCity = stockEntries.stream()
                .filter(inv -> inv.getWarehouse().getLocation().equalsIgnoreCase(customerCity))
                .filter(inv -> inv.getQuantity() > 0)
                .findFirst();

        if (sameCity.isPresent()) {
            return sameCity.get().getWarehouse();
        }

        return stockEntries.stream()
                .filter(inv -> inv.getQuantity() > 0)
                .findFirst()
                .map(Inventory::getWarehouse)
                .orElseThrow(() -> new IllegalStateException("No warehouse has stock for product " + product.getId()));
    }
}

record OrderRequest(Long productId, Long customerId, String idempotencyKey) {}
