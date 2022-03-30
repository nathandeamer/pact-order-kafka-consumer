package com.nathandeamer.kafka;

import au.com.dius.pact.consumer.MessagePactBuilder;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.consumer.junit5.ProviderType;
import au.com.dius.pact.core.model.PactSpecVersion;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.core.model.messaging.Message;
import au.com.dius.pact.core.model.messaging.MessagePact;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(PactConsumerTestExt.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@PactTestFor(providerName = "pact-order-kafka-provider", providerType = ProviderType.ASYNCH, pactVersion = PactSpecVersion.V3)
public class OrdersKafkaConsumerPactTest {

  @Autowired
  private OrderEventListener listener;

  @MockBean
  private OrderService orderService;

  @Autowired
  private ObjectMapper objectMapper;

  @Pact(consumer = "pact-order-kafka-consumer")
  MessagePact orderMessage(MessagePactBuilder builder) {
    PactDslJsonBody body = new PactDslJsonBody();
    body.integerType("id", 1234);

    body.minArrayLike("items", 1)
            .stringType("description", "Item description")
            .integerType("qty", 1)
            .stringType("sku", "ABC123")
            .closeArray();

    Map<String, Object> metadata = new HashMap<>();
    metadata.put("Content-Type", "application/json");
    metadata.put("kafka_topic", "orders");

    return builder.expectsToReceive("a order message").withMetadata(metadata).withContent(body).toPact();
  }

  @Test
  @PactTestFor(pactMethod = "orderMessage")
  void shouldConsumerOrderMessage(List<Message> messages) throws Exception {
    Order order = objectMapper.readValue(messages.get(0).contentsAsString(), Order.class);

    assertDoesNotThrow(() -> {
      listener.listen(order);
    });

    verify(orderService).processOrder(order);
  }
}