package wordCount;

import Bolt.WordCounterBolt;

import Bolt.WordSplitBolt;
import KeyGrouping.PStreamCore.Constraints;
import KeyGrouping.PStreamCore.SchedulingTopologyBuilder;
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
import org.apache.storm.tuple.Fields;


import static KeyGrouping.PStreamCore.Constraints.SCHEDULER_BOLT_ID;


public class PStreamTopology {
    public static final String WORDCOUNTER_BOLT_ID ="wordcountter-bolt";
    public static final String TOPOLOGY_NAME= "keyGroupingBalancing-topology";

    private static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers, String topic) {
        return KafkaSpoutConfig.builder(bootstrapServers, topic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                .setRetry(getRetryService())
                .setOffsetCommitPeriodMs(10_000)
                .build();
    }

    private static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        SchedulingTopologyBuilder builder=new SchedulingTopologyBuilder();
//        Integer numworkers=Integer.valueOf(7);

        builder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(Conf.KAFKA_SERVER, Conf.TOPIC_NAME)), 2);
        builder.setBolt("wordSplit", new WordSplitBolt(),3).shuffleGrouping("kafka_spout");
        builder.setDifferentiatedScheduling("wordSplit","word");
        builder.setBolt(WORDCOUNTER_BOLT_ID,new WordCounterBolt(), 7).fieldsGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.nohotFileds, new Fields(Constraints.wordFileds)).shuffleGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.hotFileds);
//        builder.setBolt(AGGREGATOR_BOLT_ID, new WordAggregatorBolt(), 36).fieldsGrouping(WORDCOUNTER_BOLT_ID, new Fields(Constraints.wordFileds));
        //Topology config
        Config config=new Config();
        config.setNumWorkers(7);
//        config.setNumWorkers(numworkers);//config numworkers

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
