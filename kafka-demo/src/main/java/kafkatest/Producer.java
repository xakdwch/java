package kafkatest;

import java.util.List;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.producer.*;

public class Producer
{
    public static void main( String[] args )
    {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            String topicName = res.getString("topic");
            long numRecords = res.getLong("numrecords");
            int recordSize = res.getInt("recordsize");
            List<String> producerProps = res.getList("props");

            Properties props = new Properties();
            if (producerProps != null) {
                for (String prop : producerProps) {
                    String[] pieces = prop.split("=");
                    if (pieces.length != 2) {
                        throw new IllegalArgumentException("Invalid property: " + prop);
                    }
                    props.put(pieces[0], pieces[1]);
                }
            }

            props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
/*
            Iterator<Entry<Object, Object>> it = props.entrySet().iterator();
            while (it.hasNext()) {
                Entry entry = (Entry)it.next();
                Object key = entry.getKey();
                Object value = entry.getValue();
                System.out.println(key + ": " + value);
            }
*/
            KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);
            byte[] recordPayload = new byte[recordSize];
            Arrays.fill(recordPayload, (byte)'x');
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topicName, recordPayload);

            for (int i = 0; i < numRecords; i++) {
                producer.send(record,
                              new Callback() {
                                  public void onCompletion(RecordMetadata metadata, Exception e) {
                                      if (e != null) {
                                          e.printStackTrace();
                                      }
                                      System.out.println("Send the message conpletely.");
                                  }
                              });
            }

            producer.close();
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

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Producer-Test").defaultHelp(true).description("This is a producer demo.");

        parser.addArgument("--topic").action(store()).required(true).type(String.class).metavar("TOPIC")
              .help("produce messages to the target topic");

        parser.addArgument("--num-records").action(store()).required(true).type(Long.class).metavar("NUM-RECORDS")
              .dest("numrecords").help("number of messages to produce.");

        parser.addArgument("--record-size").action(store()).required(true).type(Integer.class).metavar("RECORD-SIZE")
              .dest("recordsize").help("message size in bytes.");

        parser.addArgument("--props").nargs("+").required(true).type(String.class).metavar("PROP-KEY=PROP-VALUE")
              .help("kafka producer configs, like bootstrap.servers=, acks=, etc.");

        return parser;
    }
}
