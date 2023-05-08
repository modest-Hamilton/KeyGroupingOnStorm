package reviewProcess;

import Bolt.ReviewProcessBolt;
import Bolt.ReviewSplitBolt;

import KeyGrouping.PStreamForReview.Constraints;
import KeyGrouping.PStreamForReview.SchedulingTopologyBuilder;
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

import static KeyGrouping.PStream.core.Constraints.SCHEDULER_BOLT_ID;


public class PStreamTopology {
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

        builder.setSpout("kafka_spout", new KafkaSpout<>(getKafkaSpoutConfig(Conf.KAFKA_SERVER, Conf.TOPIC_NAME)), 5);
        builder.setBolt("reviewSplit", new ReviewSplitBolt()).shuffleGrouping("kafka_spout");
        builder.setDifferentiatedScheduling("reviewSplit","product_id");
        builder.setBolt("reviewResult",new ReviewProcessBolt(), 36).fieldsGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.nohotFileds, new Fields("product_id")).shuffleGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.hotFileds);
        //Topology config
        Config config=new Config();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 11 * 60);
//        config.setNumWorkers(numworkers);//config numworkers
        if(args[0].equals("local")){
            LocalCluster localCluster=new LocalCluster();

            localCluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
//            Utils.sleep(50*1000);//50s
//            localCluster.killTopology(TOPOLOGY_NAME);
//            localCluster.shutdown();
        }else {
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }

    }
}