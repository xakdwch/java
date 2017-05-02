package kafkatest;

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;

public class ConsumerGroup {
    private List<ConsumerRunner> consumers;

    public ConsumerGroup(int numThreads, String topicName, Properties props) {
        consumers = new ArrayList<ConsumerRunner>(numThreads);

        for (int i = 0; i < numThreads; i++) {
            ConsumerRunner consumerThread = new ConsumerRunner(topicName, props);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        for (ConsumerRunner consumer : consumers) {
            new Thread(consumer).start();
        }
    }
}
