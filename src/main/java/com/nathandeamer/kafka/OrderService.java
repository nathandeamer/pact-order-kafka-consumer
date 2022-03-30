package com.nathandeamer.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderService {

    public void processOrder(Order order) {
      log.info("Processing order: {}", order);
    }
}
