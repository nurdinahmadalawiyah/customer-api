package com.example.customerapi.service;

import com.example.customerapi.model.Customer;
import com.example.customerapi.repository.CustomerRepository;
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

    private static final long REDIS_EXPIRATION_TIME = 60 * 60;

    public List<Customer> getAllCustomers() {
        logger.info("Fetching all customers from the database");
        return customerRepository.findAll();
    }

    public Customer createCustomer(Customer customer) {
        Customer savedCustomer = customerRepository.save(customer);
        logger.info("Saving customer with id: " + savedCustomer.getId() + " to Redis");
        redisTemplate.opsForValue().set("customers::" + savedCustomer.getId(), savedCustomer, REDIS_EXPIRATION_TIME, TimeUnit.SECONDS);
        return savedCustomer;
    }

    public Optional<Customer> getCustomerById(Long id) {
        logger.info("Fetching customer with id: " + id + " from Redis");
        Customer customer = (Customer) redisTemplate.opsForValue().get("customers::" + id);
        if (customer != null) {
            return Optional.of(customer);
        } else {
            logger.info("Customer with id: " + id + " not found in Redis, fetching from database");
            Optional<Customer> customerFromDb = customerRepository.findById(id);
            if (customerFromDb.isPresent()) {
                logger.info("Saving customer with id: " + id + " to Redis after fetching from database");
                redisTemplate.opsForValue().set("customers::" + id, customerFromDb.get(), REDIS_EXPIRATION_TIME, TimeUnit.SECONDS);
            }
            return customerFromDb;
        }
    }

    public Customer updateCustomer(Long id, Customer customer) {
        customer.setId(id);
        Customer updatedCustomer = customerRepository.save(customer);
        logger.info("Updating customer with id: " + id + " in Redis");
        redisTemplate.opsForValue().set("customers::" + id, updatedCustomer, REDIS_EXPIRATION_TIME, TimeUnit.SECONDS);
        return updatedCustomer;
    }

    public void deleteCustomer(Long id) {
        logger.info("Deleting customer with id: " + id + " from Redis");
        redisTemplate.delete("customers::" + id);
        customerRepository.deleteById(id);
    }
}