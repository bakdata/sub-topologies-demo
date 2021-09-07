package com.bakdata.kafka;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
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
        final StreamsConfig config = new StreamsConfig(this.getKafkaProperties());

        this.customerOrderTopology = CustomerOrderTopology.builder()
                .streamsConfigs(config.originals())
                .getInputTopicByRole(this::getInputTopic)
                .build();

        this.topologyPart
                .buildTopology(this.customerOrderTopology, builder, this.getInputTopics(), this.getOutputTopic());
    }

    @Override
    public String getUniqueAppId() {
        return "sub-topologies-" + this.getOutputTopic();
    }

    @Override
    protected Properties createKafkaProperties() {
        final Properties kafkaProperties = super.createKafkaProperties();
        kafkaProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        kafkaProperties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, CustomRocksDBConfig.class);
        return kafkaProperties;
    }

    @RequiredArgsConstructor
    enum TopologyPart {
        CUSTOMERS_LOOKUP {
            @Override
            public void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                    final List<String> inputTopics, final String outputTopic) {
                final KStream<String, Order> input = builder.stream(inputTopics);

                customerOrderTopology.buildOrderCustomerJoiner(builder, input)
                        .to(outputTopic);
            }
        },
        LONG_RUNNING {
            @Override
            public void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                    final List<String> inputTopics, final String outputTopic) {
                final KStream<String, JoinedOrderCustomer> input = builder.stream(inputTopics);

                CustomerOrderTopology.buildLongRunningSubtopolgy(input)
                        .to(outputTopic);
            }
        },
        ALL {
            @Override
            public void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                    final List<String> inputTopics, final String outputTopic) {
                final KStream<String, Order> input = builder.stream(inputTopics);

                final KStream<String, JoinedOrderCustomer> joinedPageviewUserStream =
                        customerOrderTopology.buildOrderCustomerJoiner(builder, input)
                                .repartition();

                CustomerOrderTopology.buildLongRunningSubtopolgy(joinedPageviewUserStream)
                        .to(outputTopic);
            }
        };

        abstract void buildTopology(final CustomerOrderTopology customerOrderTopology, final StreamsBuilder builder,
                final List<String> inputTopics, final String outputTopic);
    }

    public static class CustomRocksDBConfig implements RocksDBConfigSetter {
        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        }
    }
}
