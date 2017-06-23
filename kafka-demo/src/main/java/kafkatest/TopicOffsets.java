package kafkatest;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import kafka.network.BlockingChannel;
import kafka.common.TopicAndPartition;
import kafka.javaapi.PartitionMetadata;

import kafkatest.network.NetworkChannel;
import kafkatest.util.metadata.TopicMetadataFetcher;
import kafkatest.util.metadata.TopicOffsetsFetcher;

public class TopicOffsets {
    public static void main(String [] argv) {
        ArgumentParser parser = argparser();
        BlockingChannel channel = null;
        Map<Integer, PartitionMetadata> partitionMetadata = null;
        Map<Integer, Long> topicLogEndOffset = new HashMap<Integer, Long>();

        try {
            Namespace res = parser.parseArgs(argv);
            String [] brokerAddr = res.getString("brokeraddr").split(":");
            String brokerHost = brokerAddr[0];
            int brokerPort = Integer.parseInt(brokerAddr[1]);
            String topic = res.getString("topic");
            int partitionId = res.getInt("partition");

            channel = NetworkChannel.channelToBroker(brokerHost, brokerPort);
            partitionMetadata = TopicMetadataFetcher.fetchTopicMetadata(channel, topic);
            if (partitionId < 0) {
                Iterator<Map.Entry<Integer, PartitionMetadata>> itr = partitionMetadata.entrySet().iterator();
                while (itr.hasNext()) {
                    channel.disconnect();
                    Map.Entry<Integer, PartitionMetadata> entry = itr.next();
                    channel = NetworkChannel.channelToBroker(entry.getValue().leader().host(), entry.getValue().leader().port());

                    long offset = TopicOffsetsFetcher.fetchTopicOffsets(channel, topic, entry.getKey());
                    topicLogEndOffset.put(entry.getKey(), offset);
                }
            }
            else {
                PartitionMetadata pm = partitionMetadata.get(partitionId);
                if (pm == null) {
                    System.out.println(String.format("Invalid partition id: %d", partitionId));
                    System.exit(1);
                }
                if (!brokerHost.equals(pm.leader().host()) || brokerPort != pm.leader().port()) {
                    channel.disconnect();
                    channel = NetworkChannel.channelToBroker(pm.leader().host(), pm.leader().port());
                }

                long offset = TopicOffsetsFetcher.fetchTopicOffsets(channel, topic, partitionId);
                topicLogEndOffset.put(partitionId, offset);
            }
            displayLogEndOffset(topic, topicLogEndOffset);
        }
        catch (ArgumentParserException e) {
            if (argv.length == 0) {
                parser.printHelp();
                System.exit(0);
            }
            else {
                parser.handleError(e);
                System.exit(1);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (channel != null) {
                channel.disconnect();
                channel = null;
            }
        }
    }

    private static void displayLogEndOffset(String topic,  Map<Integer, Long> topicLogEndOffset) {
        Iterator<Map.Entry<Integer, Long>> itr = topicLogEndOffset.entrySet().iterator();
        System.out.println(String.format("%-15s %-15s", "Partition", "LogEndOffset"));
        while (itr.hasNext()) {
            Map.Entry<Integer, Long> entry = itr.next();
            System.out.println(String.format("%s-%02d: %15d", topic, entry.getKey(), entry.getValue()));
        }
    }

    private static ArgumentParser argparser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("TopicOffsets").defaultHelp(true).
                description("This is topic offsets demo.");
        parser.addArgument("--broker-addr").action(store()).required(true).type(String.class).
                metavar("BROKER-ADDR").dest("brokeraddr").help("broker address to connect to.");
        parser.addArgument("--topic").action(store()).required(true).type(String.class).
                metavar("TOPIC").help("the target topic.");
        parser.addArgument("--partition").action(store()).required(false).type(Integer.class).
                metavar("PARTITION").setDefault(-1).help("the specific partition, default all the partitions.");

        return parser;
    }
}
