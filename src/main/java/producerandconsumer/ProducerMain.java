package producerandconsumer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class ProducerMain {

    public static void main(String... args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // props.put("acks", "0"); // producer doesn't wait for the response of the broker
        // props.put("acks", "1"); // producer will consider the write successful when the leader acknowledged
        props.put("acks", "all"); // acknowledged when all in-sync replicas have acknowledged the record

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            int partition = 0;
            if (i > 49) {
                partition = 1;
            }
            String value = Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<>("test_count", partition, "count", value);
            producer.send(record, (RecordMetadata metadata, Exception e) -> {
                if (e != null) {
                    System.out.println(e.getMessage());
                } else {
                    System.out.println("Published message: key=" + record.key() + " value=" + record.value()
                            + " partition=" + metadata.partition() + " offset=" + metadata.offset());
                }
            });
        }

        producer.close();
    }

}
