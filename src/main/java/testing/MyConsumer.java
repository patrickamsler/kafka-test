package testing;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 *
 * @author will
 */
public class MyConsumer {
    
    private Consumer<Integer, String> consumer;

    public static MyConsumer createMyConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "group1");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        MyConsumer myConsumer = new MyConsumer(new KafkaConsumer<>(props));
        return myConsumer;
    }

    public MyConsumer(Consumer<Integer, String> consumer) {
        this.consumer = consumer;
    }
    
    public void run() {
        consumer.subscribe(Arrays.asList("test_topic"));
        
        while (true) {
            handleRecords();
        }
        
    }
    
    public void handleRecords() {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("key=" + record.key() + ", value=" + record.value() + ", topic=" + record.topic() + ", partition=" + record.partition() + ", offset=" + record.offset());
        }
        consumer.commitSync();
    }
    
}