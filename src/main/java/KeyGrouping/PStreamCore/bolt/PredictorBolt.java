package KeyGrouping.PStreamCore.bolt;

import KeyGrouping.PStreamCore.Constraints;
import KeyGrouping.PStreamCore.inter.DumpRemoveHandler;
import KeyGrouping.PStreamCore.util.PredictorHotKeyUtil;
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
    private PredictorHotKeyUtil predictorHotKeyUtil=PredictorHotKeyUtil.getPredictorHotKeyUtilInstance();
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        final String word = tuple.getStringByField(Constraints.wordFileds);
        Integer count = tuple.getIntegerByField(Constraints.coinCountFileds);

        predictorHotKeyUtil.PredictorHotKey(word,count);

        if(predictorHotKeyUtil.isHotKey(word))
            collector.emit(new Values(word,1));

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
        declarer.declare(new Fields(Constraints.wordFileds,Constraints.typeFileds));
    }
}
