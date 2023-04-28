package Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordAggregatorBolt extends AggregatorBolt {
    private Map<String, Long> counts = new HashMap<String, Long>();

    @Override
    public void AggregatorFunc(OutputCollector outputCollector, Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = counts.get(word);
        if (count == null) {
            count = 0L;
        }
        count+=tuple.getLongByField("count");
        counts.put(word,count);
    }

    @Override
    public void declareFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word,count"));
    }
}
