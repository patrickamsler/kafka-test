package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StatelessTransformationsMain {

    public static void main(String...args) throws InterruptedException {
        // Set up the configuration.
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-transformations-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("stateless-transformations-input-topic");

        // split source stream into multiple streams
        KStream<String, String>[] branches = source.branch(
                (key, value) -> key.startsWith("a"), // branch 1
                (key, value) -> key.startsWith("b"), // branch 2
                (key, value) -> true // branch 3
        );

        KStream<String, String> aKeyStream = branches[0];
        KStream<String, String> bKeyStream = branches[1];
        KStream<String, String> otherKeyStream = branches[2];

        aKeyStream = aKeyStream.filter((key, value) -> value.startsWith("a")); // filter out if value doesn't begin with a

        // For the "a" stream, convert each record into two records, one with an uppercased value and one wit
        aKeyStream = aKeyStream.flatMap((key, value) -> {
            LinkedList<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, value.toUpperCase()));
            result.add(KeyValue.pair(key, value.toLowerCase()));
            return result;
        });

        // modify all keys
        aKeyStream = aKeyStream.map((key, value) -> KeyValue.pair(key.toUpperCase(), value));

        // merge all streams back together
        KStream<String, String> mergedStream = aKeyStream.merge(bKeyStream).merge(otherKeyStream);

        mergedStream = mergedStream.peek((key, value) -> System.out.println(key + " " + value));

        mergedStream.to("stateless-transformations-output-topic");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);

        // Print the topology to the console.
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach a shutdown handler to catch control-c and terminate the application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        streams.start();
        latch.await(); // wait till program ends
    }

}
