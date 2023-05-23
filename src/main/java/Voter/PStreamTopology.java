package Voter;

import Bolt.VoterProcessBolt;
import Bolt.VoterSplitBolt;
import Bolt.ZipfDataCounterBolt;
import KeyGrouping.PStreamForVote.Constraints;
import KeyGrouping.PStreamForVote.SchedulingTopologyBuilder;
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
        builder.setBolt("voteSplit", new VoterSplitBolt(),3).shuffleGrouping("kafka_spout");
        builder.setDifferentiatedScheduling("voteSplit","zip_code");
        builder.setBolt("voteCounter",new VoterProcessBolt(), 7).fieldsGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.nohotFileds, new Fields("zip_code")).shuffleGrouping(SCHEDULER_BOLT_ID+builder.getSchedulingNum(), Constraints.hotFileds);
        //Topology config
        Config config=new Config();
//        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 11 * 60);
        config.setNumWorkers(7);//config numworkers
        if(args.length > 0 && args[0].equals("local")){
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        }else {
            StormSubmitter.submitTopology("ClusterReadingFromKafkaApp",config,builder.createTopology());
        }
    }
}
