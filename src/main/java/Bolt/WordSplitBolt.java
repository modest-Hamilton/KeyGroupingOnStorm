package Bolt;

import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordSplitBolt extends BaseRichBolt {
    private OutputCollector collector;
    boolean stop;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.stop = false;
    }

    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getStringByField("value");
        if(line.length() == 0) {
            return;
        }
//        System.out.println("recv from kafka:" + line);
        long inTime = 0;
        String[] words = line.split(" ");
        for(String word : words){
            word = word.trim();
            if(!word.isEmpty()){
                word = word.toLowerCase();
                collector.emit(tuple, new Values(word));
            }
        }
        collector.ack(tuple);
    }

    public void cleanup() {}

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
