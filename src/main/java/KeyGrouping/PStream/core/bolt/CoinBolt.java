package KeyGrouping.PStream.core.bolt;

import KeyGrouping.PStream.core.Constraints;
import KeyGrouping.PStream.core.util.PredictorHotKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static KeyGrouping.PStream.core.Constraints.Threshold_r;


public class CoinBolt extends BaseRichBolt {
    private OutputCollector collector;
    private PredictorHotKeyUtil predictorHotKeyUtil=PredictorHotKeyUtil.getPredictorHotKeyUtilInstance();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField(Constraints.wordFileds);
        int coincount = predictorHotKeyUtil.countCointUtilUp();
        if(coincount>=Threshold_r)
            collector.emit(new Values(word,coincount));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(Constraints.wordFileds,Constraints.coinCountFileds));
    }

}
