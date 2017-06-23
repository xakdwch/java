package kafkatest;

import java.util.List;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import kafkatest.util.consumer.ConsumerRunner;
import kafkatest.util.consumer.ConsumerGroup;

public class Consumer {
    public static void main(String[] args) {
        ArgumentParser parser = argparser();

        try {
            Namespace res = parser.parseArgs(args);

            String topicName = res.getString("topic");
            int numThreads = res.getInt("numthreads");
            List<String> consumerProps = res.getList("props");

            Properties props = new Properties();
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.offset.reset", "earliest");
            props.put("session.timeout.ms", "30000");
            props.put("print.key", "true");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            if (consumerProps != null) {
                for (String prop : consumerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2) {
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    }
                    props.put(pieces[0], pieces[1]);
                }
            }

            ConsumerGroup consumerGroup = new ConsumerGroup(numThreads, topicName, props);
            consumerGroup.execute();
        }
        catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            }
            else {
                parser.handleError(e);
                System.exit(1);
            }
        }
    }

    private static ArgumentParser argparser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Consumer-Test").defaultHelp(true).description("This is a consumer demo.");

        parser.addArgument("--topic").action(store()).required(true).type(String.class).metavar("TOPIC")
              .help("consume message from the target topic.");

        parser.addArgument("--num-threads").action(store()).required(true).type(Integer.class).metavar("NUM-THREADS")
              .dest("numthreads").help("number of consumers to consume message.");

        parser.addArgument("--props").nargs("+").required(true).type(String.class).metavar("PROP-KEY=PROP-VALUE")
              .help("kafka consumer configuration, like bootstrap.servers=, group.id=, etc.");

        return parser;
    }
}
