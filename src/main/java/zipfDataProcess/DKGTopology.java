package zipfDataProcess;

import Bolt.ZipfDataCounterBolt;
import Bolt.ZipfDataSplitBolt;
import KeyGrouping.DKGrouping_string.DKGStorm;
import KeyGrouping.DKGrouping_string.SKey;
import Util.Conf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;
import java.util.List;

public class DKGTopology {
    private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers, String topic) {
        return KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setRetry(getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .build();
    }

    // 定义重试策略
    private static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    public static void main(String[] args) throws InterruptedException {
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(Conf.KAFKA_SERVER, Conf.TOPIC_NAME)), 2);
        builder.setBolt("zipfSplit", new ZipfDataSplitBolt()).shuffleGrouping("kafka_spout");

        double theta = 0.1;
        double factor = 1;
        int learningLength = 6000000; //20000000 * 0.6
        class Key implements SKey, Serializable {
            @Override
            public String get(List<Object> values) {
                return values.get(0).toString();
            }
        };

        builder.setBolt("zipfResult", new ZipfDataCounterBolt(), 7).customGrouping("zipfSplit",
                new DKGStorm(theta, factor, learningLength, new Key()));

        Config config = new Config();
//        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 11 * 60);
        config.setNumWorkers(7);


        if (args.length > 0 && args[0].equals("local")) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("LocalReadingFromKafkaApp",
                    config, builder.createTopology());
        } else {
            try {
                StormSubmitter.submitTopology("ClusterReadingFromKafkaApp", config, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}
