package com.geekshirt.shippingservice.consumer;

import com.geekshirt.shippingservice.dto.ShipmentOrderRequest;
import com.geekshirt.shippingservice.service.ShipmentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ShipmentOrderKafkaConsumer {
    @Autowired
    private ShipmentService shipmentService;

    @StreamListener(Sink.INPUT)
    public void consume(final ShipmentOrderRequest in) {
        log.debug(" [x] Received '" + in.getOrderId() + "'");
        shipmentService.createShipment(in);
    }
}
