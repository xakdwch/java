package kafkatest;

import java.util.Properties;
import java.util.Map;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import kafka.network.BlockingChannel;

import kafkatest.network.NetworkChannel;
import kafkatest.util.metadata.TopicMetadataFetcher;

public class TopicMetadata {
    public static void main(String [] args) {
        ArgumentParser parser = argparser();
        BlockingChannel channel = null;

        try {
            Namespace res = parser.parseArgs(args);

            String [] brokerAddr = res.getString("brokeraddr").split(":");
            String brokerHost = brokerAddr[0];
            int brokerPort = Integer.parseInt(brokerAddr[1]);
            String topic = res.getString("topic");

            channel = NetworkChannel.channelToBroker(brokerHost, brokerPort);
            TopicMetadataFetcher.fetchTopicMetadata(channel, topic);
        }
        catch (ArgumentParserException e){
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
            }
        }
    }

    private static ArgumentParser argparser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("TopicMetadata").defaultHelp(true).
                description("This is topic metadata demo.");
        parser.addArgument("--broker-addr").action(store()).required(true).type(String.class).
                metavar("BROKER-ADDR").dest("brokeraddr").help("broker address to connect to.");
        parser.addArgument("--topic").action(store()).required(true).type(String.class).
                metavar("TOPIC").help("the target topic");

        return parser;
    }
}

