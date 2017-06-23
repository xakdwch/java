package kafkatest.util.metadata;

import java.nio.ByteBuffer;

import kafka.network.BlockingChannel;
import kafka.javaapi.OffsetCommitResponse;
import kafka.javaapi.OffsetCommitRequest;
import kafka.javaapi.Implicits;

public class OffsetCommiter {
    public static OffsetCommitResponse commitOffset(BlockingChannel channel, OffsetCommitRequest request) {
        try {
            channel.send(request.underlying());
            return getOffsetResponse(channel.receive().payload());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static OffsetCommitResponse getOffsetResponse(ByteBuffer payload) {
        return Implicits.toJavaOffsetCommitResponse(kafka.api.OffsetCommitResponse.readFrom(payload));
    }
}
