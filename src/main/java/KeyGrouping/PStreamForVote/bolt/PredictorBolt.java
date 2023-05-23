package KeyGrouping.PStreamForVote.bolt;

import KeyGrouping.PStreamForVote.Constraints;
import KeyGrouping.PStreamForVote.inter.DumpRemoveHandler;
import KeyGrouping.PStreamForVote.util.PredictorHotKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class PredictorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private PredictorHotKeyUtil predictorHotKeyUtil= PredictorHotKeyUtil.getPredictorHotKeyUtilInstance();
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        final String zip_code = tuple.getStringByField("zip_code");
        Integer count = tuple.getIntegerByField(Constraints.coinCountFileds);

        predictorHotKeyUtil.PredictorHotKey(zip_code,count);

        if(predictorHotKeyUtil.isHotKey(zip_code))
            collector.emit(new Values(zip_code,1));

        predictorHotKeyUtil.SynopsisHashMapRandomDump(new DumpRemoveHandler() {
            @Override
            public void dumpRemove(String key) {
                collector.emit(new Values(key,0));
            }
        });

        collector.ack(tuple);
    }

    public void cleanup(){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("zip_code", Constraints.typeFileds));
    }
}
