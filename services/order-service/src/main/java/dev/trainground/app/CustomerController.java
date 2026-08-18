package dev.trainground.app;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Entity
class Customer {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String city;

    public Customer() {}
    public Customer(String name, String city) {
        this.name = name;
        this.city = city;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getCity() { return city; }
}

interface CustomerRepository extends JpaRepository<Customer, Long> {}

@RestController
@RequestMapping("/customers")
class CustomerController {
    private final CustomerRepository repository;

    CustomerController(CustomerRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    Page<Customer> all(@RequestParam(defaultValue = "0") int page,
                        @RequestParam(defaultValue = "100") int size) {
        return repository.findAll(PageRequest.of(page, size));
    }

    @PostMapping
    Customer create(@RequestBody Customer customer) {
        return repository.save(customer);
    }
}
