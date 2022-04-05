package com.nathandeamer.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderEventListener {

	private final OrderService orderService;

	@KafkaListener(topics = "orders")
	public void listen(Order order) throws Exception {
		log.info("received order event: {}", order);
		orderService.processOrder(order);
	}
}