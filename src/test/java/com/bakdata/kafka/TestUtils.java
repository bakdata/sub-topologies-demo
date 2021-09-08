package com.bakdata.kafka;

import com.bakdata.fluent_kafka_streams_tests.TestTopology;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jooq.lambda.Seq;

@UtilityClass
public class TestUtils {

    Order getTestOrder(final String orderId, final String customerId,
            final String productId) {
        return Order.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setProductId(productId)
                .setTotalPrice(0.0f)
                .build();
    }

    Customer getTestCustomer(final String customerId, final String region) {
        return Customer.newBuilder()
                .setCustomerId(customerId)
                .setRegion(region)
                .build();
    }

    <T> List<ProducerRecord<String, T>> getOutput(
            final TestTopology<String, SpecificRecord> topology, final Class<T> valueType, final String outputTopic) {
        return Seq.seq(topology.streamOutput(outputTopic)
                .withValueType(valueType))
                .toList();
    }
}
