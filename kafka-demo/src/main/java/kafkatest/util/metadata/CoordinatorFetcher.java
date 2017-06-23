package kafkatest.util.metadata;

import kafka.network.BlockingChannel;
import kafka.cluster.BrokerEndPoint;
import kafka.api.GroupCoordinatorRequest;
import kafka.javaapi.GroupCoordinatorResponse;

public class CoordinatorFetcher {
    public static BrokerEndPoint fetchCoordinator(BlockingChannel channel, String groupId, String clientId) {
        GroupCoordinatorRequest request = new GroupCoordinatorRequest(groupId, 
                GroupCoordinatorRequest.CurrentVersion(), 0, clientId);

        channel.send(request);
        GroupCoordinatorResponse response = GroupCoordinatorResponse.readFrom(channel.receive().payload());
        return response.coordinator();
    }
}
