package KeyGrouping.PStreamForZipf.bolt;

import KeyGrouping.PStreamForZipf.Constraints;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SchedulerBolt extends BaseRichBolt {
    private String UPSTREAM_COMPONENT_ID;
    private String UPSTREAM_FIELDS;
    private OutputCollector collector;
    private CountingBloomFilter bf;

    public SchedulerBolt(String UPSTREAM_COMPONENT_ID,String UPSTREAM_FIELDS) {
        this.UPSTREAM_COMPONENT_ID = UPSTREAM_COMPONENT_ID;
        this.UPSTREAM_FIELDS=UPSTREAM_FIELDS;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.bf = new CountingBloomFilter(16,4,1);
    }

    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals(UPSTREAM_COMPONENT_ID)){
//            long inTime = tuple.getLongByField("inTime");
            String key = tuple.getStringByField(UPSTREAM_FIELDS);
            collector.emit(Constraints.coinFileds, new Values(key));
            Key ky = new Key(key.getBytes());
            if(bf.membershipTest(ky))
                collector.emit(Constraints.hotFileds, tuple, new Values(key));
            else
                collector.emit(Constraints.nohotFileds, tuple, new Values(key));

        }else {
            String key = tuple.getStringByField("num");
            Integer type = tuple.getIntegerByField(Constraints.typeFileds);
            Key hk = new Key(key.getBytes());
            if(!bf.membershipTest(hk) && type.equals(1))
                bf.add(hk);
            if(bf.membershipTest(hk) && type.equals(0))
                bf.delete(hk);
        }
        collector.ack(tuple);
    }

    public void cleanup(){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constraints.coinFileds, new Fields("num"));
        declarer.declareStream(Constraints.hotFileds, new Fields("num"));
        declarer.declareStream(Constraints.nohotFileds, new Fields("num"));
    }

}
