package streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

public class KTableExample {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-example");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

    startKafkaStreams(props);
  }

  public static void startKafkaStreams(Properties props) {
    StreamsBuilder builder = new StreamsBuilder();

    KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("ktable-store");
//    KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("ktable-store");
    Materialized<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>> materialized =
        Materialized.<String, String>as(storeSupplier)
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.String());

    // Create a KTable from the input topic and configure it with the state store
    builder.table("input-topic", materialized)
        .filter((key, value) -> Long.parseLong(value) > 1000)
        .toStream()
        .peek((key, value) -> System.out.println("Outgoing record - key " + key + " value " + value))
        .to("output-topic");

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, props);
    streams.start();

    // Add shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
