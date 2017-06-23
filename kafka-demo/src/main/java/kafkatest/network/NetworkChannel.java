package kafkatest.network;

import kafka.network.BlockingChannel;

public class NetworkChannel {
    public static BlockingChannel channelToBroker(String host, int port) {
        boolean isConnected = false;
        BlockingChannel channel = null;
        while (!isConnected) {
            try {
                channel = new BlockingChannel(host, port, 
                        BlockingChannel.UseDefaultBufferSize(),
                        BlockingChannel.UseDefaultBufferSize(), 5000);
                channel.connect();
                isConnected = true;
            }
            catch (Exception e) {
                if (channel != null) {
                    channel.disconnect();
                }
                channel = null;
                isConnected = false;
                System.out.println(String.format("Error while creating channel to %s:%d", host, port));
            }
        }
        return channel;
    }
}
