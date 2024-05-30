package com.example.customerapi.repository.elasticsearch;

import com.example.customerapi.model.Customer;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface CustomerSearchRepository extends ElasticsearchRepository<Customer, Long> {
    List<Customer> findByName(String name);
}
