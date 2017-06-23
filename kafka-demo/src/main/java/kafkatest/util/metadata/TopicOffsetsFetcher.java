package kafkatest.util.metadata;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.nio.ByteBuffer;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.network.BlockingChannel;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.Implicits;

public class TopicOffsetsFetcher {
    public static long fetchTopicOffsets(BlockingChannel channel, String topic, int partition) {
        try {
            Map<TopicAndPartition, PartitionOffsetRequestInfo> req = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
            req.put(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1));
            OffsetRequest request = new OffsetRequest(req, kafka.api.OffsetRequest.CurrentVersion(), "topic-offsets-fetch-consumer");
            //System.out.println(String.format("%s", request.toString()));
            channel.send(request.underlying());
            OffsetResponse response = getOffsetResponse(channel.receive().payload());
            //System.out.println(String.format("%s", response.toString()));
            return response.offsets(topic, partition)[0];
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static OffsetResponse getOffsetResponse(ByteBuffer payload) {
        return Implicits.toJavaOffsetResponse(kafka.api.OffsetResponse.readFrom(payload));
    }
}
