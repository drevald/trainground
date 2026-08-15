package dev.trainground.app;

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

@RestController
@RequestMapping("/warehouses")
class WarehouseController {

    private WarehouseRepository repository;

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
