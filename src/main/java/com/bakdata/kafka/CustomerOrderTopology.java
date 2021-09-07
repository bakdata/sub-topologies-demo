package com.bakdata.kafka;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

@Slf4j
@Builder
public class CustomerOrderTopology {
    private static final long WAIT_MS = Duration.ofSeconds(1).toMillis();
    private final @NonNull Function<String, String> getInputTopicByRole;
    private final @NonNull Map<String, Object> streamsConfigs;


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

    public KStream<String, JoinedOrderCustomer> buildOrderCustomerJoiner(final StreamsBuilder builder,
            final KStream<String, Order> input) {

        final KTable<String, Customer> customerKTable = builder.<String, Customer>stream(this.getInputTopic("customers"))
                .toTable(Named.as("customers-ktable"));

        return input
                .selectKey((key, value) -> value.getCustomerId()) // repartition to customerId for lookup
                .leftJoin(customerKTable, CustomerOrderTopology::join);
    }

    private String getInputTopic(final String topic) {
        return this.getInputTopicByRole.apply(topic);
    }
}
