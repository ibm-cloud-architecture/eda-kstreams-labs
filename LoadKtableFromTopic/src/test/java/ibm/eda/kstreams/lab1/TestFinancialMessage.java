package ibm.eda.kstreams.lab1;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import ibm.eda.kstreams.lab.domain.FinancialMessage;
import ibm.eda.kstreams.lab.infra.JSONSerde;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class TestFinancialMessage {

    private static TopologyTestDriver testDriver;
    private static String inTopicName = "transactions";
    private static String outTopicName = "output";
    private static String errorTopicName = "errors";
    private static String storeName = "transactionCount";
    private static TestInputTopic<String, FinancialMessage> inTopic;
    private static TestOutputTopic<String, Long> outTopic;
    private static TestOutputTopic<String, String> errorTopic;

    public static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-lab2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummmy:2345");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    @BeforeAll
    public static void buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore(storeName);

        KStream<String, FinancialMessage> transactionStream = builder.stream(
                inTopicName,
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        // First verify user id is present, if not route to error
        Map<String, KStream<String,FinancialMessage>> branches = transactionStream
            .split(Named.as("tx-"))
            .branch((key, value) -> value.userId == null, Branched.as("no-userid"))
            .defaultBranch(Branched.as("non-null"));

        // Handle error by sending to the errors topic.
        branches.get("tx-no-userid").map(
                (key, value) -> {
                    return KeyValue.pair(key, "No customer id provided");
                })
                .to(
                        errorTopicName, Produced.with(Serdes.String(), Serdes.String()));

        // use groupBy to swap the key, then count by customer id,
        branches.get("tx-non-null")
                .filter((key, value) -> (value.totalCost > 5000))
                .groupBy((key, value) -> value.userId)
                .count(
                        Materialized.as(storeSupplier))
                .toStream()
                .to(
                        outTopicName,
                        Produced.with(Serdes.String(), Serdes.Long()));

        testDriver = new TopologyTestDriver(builder.build(), getStreamsConfig());
        inTopic = testDriver.createInputTopic(inTopicName, new StringSerializer(),
                new JSONSerde<FinancialMessage>());
        // outTopic =
        // testDriver.createOutputTopic(outTopicName,windowedSerde.deserializer(), new
        // LongDeserializer());
        outTopic = testDriver.createOutputTopic(outTopicName, new StringDeserializer(), new LongDeserializer());
        errorTopic = testDriver.createOutputTopic(errorTopicName, new StringDeserializer(), new StringDeserializer());
    }

    @AfterAll
    public static void close() {
        testDriver.close();
    }

    @Test
    public void shouldHaveOneTransaction() {
        // A FinancialMessage is mocked and set to the input topic. Within the Topology,
        // this gets sent to the outTopic because a userId exists for the incoming message.

        FinancialMessage mock = new FinancialMessage(
            "1", "MET", "SWISS", 12, 1822.38, 21868.55, 94, 7, true
        );
        FinancialMessage mock2 = new FinancialMessage(
            "2", "ASDF", "HELLO", 5, 1000.22, 4444.12, 38, 6, true
        );

        inTopic.pipeInput("T01", mock);
        inTopic.pipeInput("T02", mock2);

        Assertions.assertFalse(outTopic.isEmpty());
        Assertions.assertEquals(1, outTopic.readKeyValue().value);

        KeyValueStore<String,ValueAndTimestamp<FinancialMessage>> store = testDriver.getTimestampedKeyValueStore(storeName);
        Assertions.assertEquals(1, store.approximateNumEntries());
    }

    @Test
    public void testErrorTopicIsNotEmpty() {
        FinancialMessage mock = new FinancialMessage(
            null, "MET", "SWISS", 12, 1822.38, 21868.55, 94, 7, true
        );

        inTopic.pipeInput("T03", mock);

        Assertions.assertFalse(errorTopic.isEmpty());
    }
}