package KeyGrouping.PStreamForVote.bolt;

import KeyGrouping.PStreamForVote.Constraints;
import KeyGrouping.PStreamForVote.util.PredictorHotKeyUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static KeyGrouping.PStreamCore.Constraints.Threshold_r;


public class CoinBolt extends BaseRichBolt {
    private OutputCollector collector;
    private PredictorHotKeyUtil predictorHotKeyUtil= PredictorHotKeyUtil.getPredictorHotKeyUtilInstance();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String zip_code = tuple.getStringByField("zip_code");
        int coincount = predictorHotKeyUtil.countCointUtilUp();
        if(coincount>=Threshold_r)
            collector.emit(new Values(zip_code,coincount));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("zip_code", Constraints.coinCountFileds));
    }

}
