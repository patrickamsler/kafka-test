package testing;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;

class MyConsumerTest {

    @Test
    void testMyConsumer() {
        MockConsumer<Integer, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        MyConsumer myConsumer = new MyConsumer(mockConsumer);

        mockConsumer.assign(Arrays.asList(new TopicPartition("test_topic", 0)));
        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        startOffsets.put(new TopicPartition("test_topic", 0), 0L);
        mockConsumer.updateBeginningOffsets(startOffsets);
        mockConsumer.addRecord(new ConsumerRecord<>("test_topic", 0, 1, 42, "test"));

        myConsumer.handleRecords();
    }

}