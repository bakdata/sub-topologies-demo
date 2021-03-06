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

import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import picocli.CommandLine;

@Setter
public class SplitSubTopologiesDemo extends KafkaStreamsApplication {

    private CustomerOrderTopology customerOrderTopology;
    @CommandLine.Option(names = "--topology-part", description = "Valid values: ${COMPLETION-CANDIDATES}",
            required = true)
    private TopologyPart topologyPart;


    public static void main(final String[] args) {
        startApplication(new SplitSubTopologiesDemo(), args);
    }

    @Override
    public void buildTopology(final StreamsBuilder builder) {
        this.topologyPart
                .buildTopology(this.customerOrderTopology, builder, this.getInputTopics(), this.getOutputTopic(),
                        this::getInputTopic);
    }

    @Override
    public String getUniqueAppId() {
        return "sub-topologies-" + this.getOutputTopic();
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        return kafkaProperties;
    }

    @RequiredArgsConstructor
    enum TopologyPart {
        CUSTOMERS_LOOKUP {
            @Override
            public void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                    final List<String> inputTopics, final String outputTopic,
                    final Function<String, String> getInputTopicByRole) {
                final KStream<String, Order> input = builder.stream(inputTopics);

                CustomerOrderTopology.buildOrderCustomerJoiner(builder, input, getInputTopicByRole.apply("customers"))
                        .to(outputTopic);
            }
        },
        LONG_RUNNING {
            @Override
            public void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                    final List<String> inputTopics, final String outputTopic,
                    final Function<String, String> getInputTopicByRole) {
                final KStream<String, JoinedOrderCustomer> input = builder.stream(inputTopics);

                CustomerOrderTopology.buildLongRunningSubtopolgy(input)
                        .to(outputTopic);
            }
        },
        ALL {
            @Override
            public void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                    final List<String> inputTopics, final String outputTopic,
                    final Function<String, String> getInputTopicByRole) {
                final KStream<String, Order> input = builder.stream(inputTopics);

                final KStream<String, JoinedOrderCustomer> joinedPageviewUserStream =
                        CustomerOrderTopology.buildOrderCustomerJoiner(builder, input, getInputTopicByRole.apply("customers"))
                                .repartition();

                CustomerOrderTopology.buildLongRunningSubtopolgy(joinedPageviewUserStream)
                        .to(outputTopic);
            }
        },
        ;

        abstract void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                final List<String> inputTopics, final String outputTopic,
                final Function<String, String> getInputTopicByRole);
    }
}
