package Bolt;

import KeyGrouping.OKGrouping.OKGCompiler;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class OKGroupingZipfBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 22L;
    private ArrayList<String> collectKey;
    private OKGCompiler compiler;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collectKey = new ArrayList<>();
        this.collector = outputCollector;
        this.compiler = OKGCompiler.getInstance();
    }
    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple tuple:tupleWindow.get()) {
            collectKey.add(tuple.getStringByField("num"));
        }
        compiler.getData(collectKey);
        compiler.processData();
        HashMap<String, Integer> routeTable = compiler.getRouteTable();
        for(Tuple tuple:tupleWindow.get()) {
            String key = tuple.getStringByField("num");
            int chosenTask = routeTable.get(key);
            collector.emit(tuple, new Values(chosenTask, key));
            collector.ack(tuple);
        }
        collectKey.clear();
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("chosen_task","num"));
    }
}
