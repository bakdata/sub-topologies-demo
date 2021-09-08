/*
 * MIT License
 *
 * Copyright (c) 2021 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.bakdata.kafka;

import com.bakdata.fluent_kafka_streams_tests.junit5.TestTopologyExtension;
import com.bakdata.kafka.SplitSubTopologiesDemo.TopologyPart;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.List;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(SoftAssertionsExtension.class)
class SplitSubTopologiesDemoTest {
    private static final String INPUT_TOPIC = "INPUT";
    private static final String OUTPUT_TOPIC = "OUTPUT";
    private static final Map<String, String> EXTRA_INPUT_TOPICS = Map.of(
            "customers", "CUSTOMERS"
    );
    @InjectSoftAssertions
    private SoftAssertions softly;
    private final SplitSubTopologiesDemo subTopologyDemo = createApp();

    @RegisterExtension
    final TestTopologyExtension<String, SpecificRecord> topology =
            new TestTopologyExtension<>(p -> {
                this.subTopologyDemo
                        .setSchemaRegistryUrl(p.getProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG));
                return this.subTopologyDemo.createTopology();
            }, this.subTopologyDemo.getKafkaProperties());

    private static SplitSubTopologiesDemo createApp() {
        final SplitSubTopologiesDemo demoApplication = new SplitSubTopologiesDemo();
        demoApplication.setInputTopics(List.of(INPUT_TOPIC));
        demoApplication.setExtraInputTopics(EXTRA_INPUT_TOPICS);
        demoApplication.setOutputTopic(OUTPUT_TOPIC);
        demoApplication.setTopologyPart(TopologyPart.ALL);
        return demoApplication;
    }

    @Test
    void shouldProduceOrderWithoutCustomerInfo() {
        this.topology.input(INPUT_TOPIC)
                .add("order_001", TestUtils.getTestOrder("order_001",
                        "customer_001", "product_001"));

        this.softly.assertThat(TestUtils.getOutput(
                this.topology, JoinedOrderCustomer.class, this.subTopologyDemo.getOutputTopic()))
                .hasSize(1)
                .allSatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("customer_001");
                    this.softly.assertThat(record.value().getCustomerId()).isEqualTo("customer_001");
                    this.softly.assertThat(record.value().getOrderId()).isEqualTo("order_001");
                    this.softly.assertThat(record.value().getRegion()).isNull();
                });
    }

    @Test
    void shouldJoinCustomerAndOrders() {
        this.topology
                .input(EXTRA_INPUT_TOPICS.get("customers"))
                .add("customer_001", TestUtils.getTestCustomer("customer_001", "region1"));

        this.topology.input(INPUT_TOPIC)
                .add("order_001", TestUtils.getTestOrder("order_001",
                        "customer_001", "product_001"));

        this.softly.assertThat(TestUtils.getOutput(
                this.topology, JoinedOrderCustomer.class, this.subTopologyDemo.getOutputTopic()))
                .hasSize(1)
                .allSatisfy(record -> {
                    this.softly.assertThat(record.key()).isEqualTo("customer_001");
                    this.softly.assertThat(record.value().getCustomerId()).isEqualTo("customer_001");
                    this.softly.assertThat(record.value().getOrderId()).isEqualTo("order_001");
                    this.softly.assertThat(record.value().getRegion()).isEqualTo("region1");
                });
    }
}
