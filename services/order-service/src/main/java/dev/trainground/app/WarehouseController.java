package dev.trainground.app;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Entity
class Warehouse {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String location;

    public Warehouse() {}
    public Warehouse(String name, String location) {
        this.name = name;
        this.location = location;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getLocation() { return location; }
}

interface WarehouseRepository extends JpaRepository<Warehouse, Long> {}

@Entity
@Table(name = "inventory", indexes = {
        @Index(name = "idx_inventory_product", columnList = "product_id"),
        @Index(name = "idx_inventory_warehouse", columnList = "warehouse_id")
})
class Inventory {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne
    @JoinColumn(name = "product_id")
    private Product product;

    @ManyToOne
    @JoinColumn(name = "warehouse_id")
    private Warehouse warehouse;

    private Integer quantity;

    public Inventory() {}
    public Inventory(Product product, Warehouse warehouse, Integer quantity) {
        this.product = product;
        this.warehouse = warehouse;
        this.quantity = quantity;
    }

    public Long getId() { return id; }
    public Product getProduct() { return product; }
    public Warehouse getWarehouse() { return warehouse; }
    public Integer getQuantity() { return quantity; }
}

interface InventoryRepository extends JpaRepository<Inventory, Long> {
    List<Inventory> findByProduct(Product product);
}

@RestController
@RequestMapping("/warehouses")
class WarehouseController {
    private final WarehouseRepository repository;

    WarehouseController(WarehouseRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    List<Warehouse> all() {
        return repository.findAll();
    }

    @PostMapping
    Warehouse create(@RequestBody Warehouse warehouse) {
        return repository.save(warehouse);
    }
}

record InventoryRequest(Long productId, Long warehouseId, Integer quantity) {}

@RestController
@RequestMapping("/inventory")
class InventoryController {
    private final InventoryRepository inventoryRepository;
    private final ProductRepository productRepository;
    private final WarehouseRepository warehouseRepository;

    InventoryController(InventoryRepository inventoryRepository,
                         ProductRepository productRepository,
                         WarehouseRepository warehouseRepository) {
        this.inventoryRepository = inventoryRepository;
        this.productRepository = productRepository;
        this.warehouseRepository = warehouseRepository;
    }

    @GetMapping
    List<Inventory> all() {
        return inventoryRepository.findAll();
    }

    @PostMapping
    Inventory create(@RequestBody InventoryRequest request) {
        Product product = productRepository.findById(request.productId())
                .orElseThrow(() -> new IllegalArgumentException("Unknown productId"));
        Warehouse warehouse = warehouseRepository.findById(request.warehouseId())
                .orElseThrow(() -> new IllegalArgumentException("Unknown warehouseId"));
        return inventoryRepository.save(new Inventory(product, warehouse, request.quantity()));
    }
}
