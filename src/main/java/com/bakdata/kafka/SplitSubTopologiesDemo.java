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
