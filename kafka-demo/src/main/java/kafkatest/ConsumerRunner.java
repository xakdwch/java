package kafkatest;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;

public class ConsumerRunner implements Runnable{
    private final KafkaConsumer<String, String> kafkaConsumer;

    public ConsumerRunner(String topicName, Properties props) {
        kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
    }

    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(Thread.currentThread().getName() + " consumed: parition=" + record.partition() 
                                   + ", offset=" + record.offset() + ", key=" + record.key() + ", value=" + record.value());
            }
        }
    }
}
