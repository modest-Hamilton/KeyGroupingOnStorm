package KeyGrouping.PStreamForReview.bolt;

import KeyGrouping.PStreamForReview.Constraints;
import KeyGrouping.PStreamForReview.inter.DumpRemoveHandler;
import KeyGrouping.PStreamForReview.util.PredictorHotKeyUtil;
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
        final String product_id = tuple.getStringByField("product_id");
        Integer count = tuple.getIntegerByField(Constraints.coinCountFileds);

        predictorHotKeyUtil.PredictorHotKey(product_id,count);

        if(predictorHotKeyUtil.isHotKey(product_id))
            collector.emit(new Values(product_id,1));

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
        declarer.declare(new Fields("product_id", Constraints.typeFileds));
    }
}
