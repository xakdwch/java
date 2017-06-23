package kafkatest;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import kafka.network.BlockingChannel;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.OffsetCommitResponse;

import kafkatest.network.NetworkChannel;
import kafkatest.util.metadata.OffsetCommiter;
import kafkatest.util.metadata.TopicMetadataFetcher;
import kafkatest.util.metadata.CoordinatorFetcher;

public class OffsetCommit {
    public static void main(String [] args) {
        ArgumentParser parser = argparser();
        BlockingChannel channel = null;
        Map<Integer, PartitionMetadata> partitionMetadata = null;
        final Long commitTimestamp = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP;
        final Long expireTimestamp = org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP;

        try {
            Namespace res = parser.parseArgs(args);
            String [] brokerAddr = res.getString("brokeraddr").split(":");
            String brokerHost = brokerAddr[0];
            int brokerPort = Integer.parseInt(brokerAddr[1]);
            String topic = res.getString("topic");
            int partitionId = res.getInt("partition");
            String groupId = res.getString("groupid");
            String clientId = res.getString("clientid");
            //TODO:通过TopicOffsetsFetcher.fetchTopicOffsets获取每个partition的leo,未指定offset则自动将offset设置成leo
            long offset = res.getLong("offset");

            channel = NetworkChannel.channelToBroker(brokerHost, brokerPort);
            BrokerEndPoint broker = CoordinatorFetcher.fetchCoordinator(channel, groupId, clientId);
            if (broker.host() != brokerHost || broker.port() != brokerPort) {
                channel.disconnect();
                channel = NetworkChannel.channelToBroker(broker.host(), broker.port());
            }

            OffsetAndMetadata offsetMetadata = new OffsetAndMetadata(new OffsetMetadata(offset, ""), commitTimestamp, expireTimestamp);
            Map<TopicAndPartition, OffsetAndMetadata> request = new HashMap<TopicAndPartition, OffsetAndMetadata>();

            partitionMetadata = TopicMetadataFetcher.fetchTopicMetadata(channel, topic);
            if (partitionId < 0) {
                Iterator<Map.Entry<Integer, PartitionMetadata>> itr = partitionMetadata.entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry<Integer, PartitionMetadata> entry = itr.next();
                    TopicAndPartition tp = new TopicAndPartition(topic, entry.getKey());
                    request.put(tp, offsetMetadata);
                }
            }
            else {
                PartitionMetadata pm = partitionMetadata.get(partitionId);
                TopicAndPartition tp = new TopicAndPartition(topic, partitionId);
                request.put(tp, offsetMetadata);
            }

            OffsetCommitRequest requestInfo = new OffsetCommitRequest(groupId, request, 0, clientId, kafka.api.OffsetCommitRequest.CurrentVersion());
            OffsetCommitResponse response = OffsetCommiter.commitOffset(channel, requestInfo);
            if (response != null) {
                if (response.hasError()) {
                    Iterator<Map.Entry<TopicAndPartition, OffsetAndMetadata>> itr = request.entrySet().iterator();
                    while (itr.hasNext()) {
                        Map.Entry<TopicAndPartition, OffsetAndMetadata> entry = itr.next();
                        System.out.println(String.format("Commit offset error: %s", response.errorCode(entry.getKey())));
                    }
                }
                else {
                    System.out.println(String.format("Commit offset success."));
                }
            }
        }
        catch(ArgumentParserException e) {
            if (args.length == 0) {
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

    private static ArgumentParser argparser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("OffsetCommit").defaultHelp(true).
                description("This is offset commit demo.");
        parser.addArgument("--broker-addr").action(store()).required(true).type(String.class).
                metavar("BROKER-ADDR").dest("brokeraddr").help("broker address to connect to.");
        parser.addArgument("--topic").action(store()).required(true).type(String.class).
                metavar("TOPIC").help("the target topic.");
        parser.addArgument("--partition").action(store()).required(false).type(Integer.class).
                metavar("PARTITION").setDefault(-1).help("the specific partition of the target topic.");
        parser.addArgument("--group-id").action(store()).required(true).type(String.class).
                metavar("GROUP-ID").dest("groupid").help("the specific consumer group.");
        parser.addArgument("--client-id").action(store()).required(false).type(String.class).
                metavar("CLIENT-ID").dest("clientid").setDefault("").help("the specific consumer client id.");
        parser.addArgument("--offset").action(store()).required(true).type(Long.class).
                metavar("OFFSET").help("new offset value, -1 means set to the log end offset.");

        return parser;
    }
}
