package kafkatest.util.metadata;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;

import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.network.BlockingChannel;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.Implicits;

public class TopicMetadataFetcher {
    public static Map<Integer, PartitionMetadata> fetchTopicMetadata(BlockingChannel channel, String topic) {
        Map<Integer, PartitionMetadata> partitionMetadata = new HashMap<Integer, PartitionMetadata>();
        try {
            channel.send(new TopicMetadataRequest(Arrays.asList(topic)));

            TopicMetadataResponse tmResponse = getTopicMetadataResponse(channel.receive().payload());
            if (tmResponse.topicsMetadata().size() != 1) {
                System.out.println("Topic number error!");
            }

            TopicMetadata tm = tmResponse.topicsMetadata().get(0);
            if (tm.errorCode() == ErrorMapping.NoError()) {
                //System.out.println(String.format("%s", tm.toString()));
                for (PartitionMetadata pm : tm.partitionsMetadata()) {
                    partitionMetadata.put(pm.partitionId(), pm);
                    if (pm.errorCode() == ErrorMapping.NoError()) {
                        //
                    }
                    else {
                        System.out.println(String.format("Partition metadata Error Code: %d", pm.errorCode()));
                        //
                    }
                }
            }
            else {
                //TODO retry
            }

            return partitionMetadata;
        }
        catch (Exception e) {
            //TODO reconnect
            e.printStackTrace();
        }
        return null;
    }

    private static TopicMetadataResponse getTopicMetadataResponse(ByteBuffer payload) {
        return Implicits.toJavaTopicMetadataResponse(kafka.api.TopicMetadataResponse.readFrom(payload));
    }
}
