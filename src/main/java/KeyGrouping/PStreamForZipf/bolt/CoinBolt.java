package KeyGrouping.PStreamForZipf.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import KeyGrouping.PStreamForZipf.Constraints;
import KeyGrouping.PStreamForZipf.util.PredictorHotKeyUtil;

import java.util.Map;


public class CoinBolt extends BaseRichBolt {
    private OutputCollector collector;
    private PredictorHotKeyUtil predictorHotKeyUtil= PredictorHotKeyUtil.getPredictorHotKeyUtilInstance();

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("num");
        int coincount = predictorHotKeyUtil.countCointUtilUp();
        if(coincount>= Constraints.Threshold_r)
            collector.emit(new Values(word,coincount));
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("num", Constraints.coinCountFileds));
    }

}
