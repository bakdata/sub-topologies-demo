package com.bakdata.kafka;

import java.time.Duration;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

@Slf4j
@UtilityClass
public class CustomerOrderTopology {
    private static final long WAIT_MS = Duration.ofSeconds(1).toMillis();

    public static KStream<String, JoinedOrderCustomer> buildLongRunningSubtopolgy(
            final KStream<String, ? extends JoinedOrderCustomer> input) {
        return input
                .map(CustomerOrderTopology::longRunningTask);
    }

    private static KeyValue<String, JoinedOrderCustomer> longRunningTask(final String key,
            final JoinedOrderCustomer joinedOrderCustomer) {
        try {
            Thread.sleep(WAIT_MS);

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Could not wait for {} ms", WAIT_MS, e);
        }
        return KeyValue.pair(joinedOrderCustomer.getCustomerId(), joinedOrderCustomer);
    }

    private static JoinedOrderCustomer join(final Order order, final Customer customer) {
        final JoinedOrderCustomer.Builder joined = JoinedOrderCustomer
                .newBuilder()
                .setOrderId(order.getOrderId())
                .setProductId(order.getProductId())
                .setTotalPrice(order.getTotalPrice())
                .setCustomerId(order.getCustomerId());
        if (customer != null) {
            joined.setRegion(customer.getRegion());
        }
        return joined.build();
    }

    public static KStream<String, JoinedOrderCustomer> buildOrderCustomerJoiner(final StreamsBuilder builder,
            final KStream<String, Order> input, final String customersInputTopic) {

        final KTable<String, Customer> customerKTable = builder.<String, Customer>stream(customersInputTopic)
                .toTable(Named.as("customers-ktable"));

        return input
                .selectKey((key, value) -> value.getCustomerId()) // repartition to customerId for lookup
                .leftJoin(customerKTable, CustomerOrderTopology::join);
    }

}
