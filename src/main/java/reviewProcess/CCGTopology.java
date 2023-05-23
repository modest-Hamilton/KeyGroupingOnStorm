package reviewProcess;

import Bolt.*;
import KeyGrouping.OKGrouping.OKGrouping;
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
import org.apache.storm.topology.base.BaseWindowedBolt;

public class CCGTopology {
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
        int learningLength = 1000;
        final TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(Conf.KAFKA_SERVER, Conf.TOPIC_NAME)), 2);
        builder.setBolt("reviewSplit", new ReviewSplitBolt(),3).shuffleGrouping("kafka_spout");
        builder.setBolt("reviewByCCG", new CCGReviewBolt(7).withWindow(new BaseWindowedBolt.Count(learningLength),new BaseWindowedBolt.Count(learningLength))).shuffleGrouping("reviewSplit");
        builder.setBolt("reviewResult", new ReviewProcessBolt(), 7).customGrouping("reviewByCCG", new OKGrouping());
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
