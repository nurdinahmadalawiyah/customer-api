package com.example.customerapi.service;

import com.example.customerapi.model.Customer;
import com.example.customerapi.repository.jpa.CustomerRepository;
import com.example.customerapi.repository.elasticsearch.CustomerSearchRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class CustomerService {

    private static final Logger logger = LoggerFactory.getLogger(CustomerService.class);

    @Autowired
    private CustomerRepository customerRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private CustomerSearchRepository customerSearchRepository;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    private static final long REDIS_EXPIRATION_TIME = 60 * 60;

    public List<Customer> getAllCustomers() {
        logger.info("Fetching all customers from the database");
        return customerRepository.findAll();
    }

    @Transactional
    public Customer createCustomer(Customer customer) {
        Customer savedCustomer = customerRepository.save(customer);
        try {
            logger.info("Saving customer with id: " + savedCustomer.getId() + " to Redis");
            redisTemplate.opsForValue().set("customers::" + savedCustomer.getId(), savedCustomer, REDIS_EXPIRATION_TIME, TimeUnit.HOURS);
            kafkaProducerService.sendMessage(savedCustomer);
        } catch (Exception e) {
            logger.error("Error occurred while saving customer to Redis/Kafka. Rolling back transaction.", e);
            throw e;
        }
        return savedCustomer;
    }

    public Optional<Customer> getCustomerById(Long id) {
        logger.info("Fetching customer with id: " + id + " from Redis");
        Customer customer = (Customer) redisTemplate.opsForValue().get("customers::" + id);
        if (customer != null) {
            return Optional.of(customer);
        }

        Optional<Customer> customerOptional = customerRepository.findById(id);
        if (customerOptional.isPresent()) {
            Customer foundCustomer = customerOptional.get();
            logger.info("Caching customer with id: " + id + " in Redis");
            redisTemplate.opsForValue().set("customers::" + id, foundCustomer, REDIS_EXPIRATION_TIME, TimeUnit.HOURS);
            return customerOptional;
        }
        return Optional.empty();
    }

    @Transactional
    public Customer updateCustomer(Long id, Customer customer) {
        customer.setId(id);
        Customer updatedCustomer = customerRepository.save(customer);
        try {
            logger.info("Updating customer with id: " + id + " in Redis");
            redisTemplate.opsForValue().set("customers::" + id, updatedCustomer, REDIS_EXPIRATION_TIME, TimeUnit.HOURS);
            kafkaProducerService.sendMessage(updatedCustomer);
        } catch (Exception e) {
            logger.error("Error occurred while updating customer in Redis/Kafka. Rolling back transaction.", e);
            throw e;
        }
        return updatedCustomer;
    }


    @Transactional
    public void deleteCustomer(Long id) {
        try {
            logger.info("Deleting customer with id: " + id + " from Redis and sending to Kafka");
            redisTemplate.delete("customers::" + id);
            kafkaProducerService.sendMessage("Deleted customer with id: " + id);
            customerRepository.deleteById(id);
        } catch (Exception e) {
            logger.error("Error occurred while deleting customer from Redis/Kafka. Rolling back transaction.", e);
            throw e;
        }
    }

    public List<Customer> searchCustomerByName(String name) {
        logger.info("Searching customers with name: " + name + " in Elasticsearch");
        return customerSearchRepository.findByName(name);
    }
}