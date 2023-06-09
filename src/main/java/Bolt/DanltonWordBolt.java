package Bolt;

import KeyGrouping.Dalton.ContextualBandits;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DanltonWordBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 23L;
    private ContextualBandits cbandit;
    private int numOfBatch;
    private int batchSize;
    List<Tuple> batch = new ArrayList<>();
    private int numTuplesInBatch;
    private OutputCollector collector;
    public DanltonWordBolt(int parallelism, int slide, int size, int numOfKeys, int batchSize) {
        cbandit = new ContextualBandits(parallelism, slide, size, numOfKeys);
        this.batchSize = batchSize;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        numOfBatch = 0;
        numTuplesInBatch = 0;
        this.collector = outputCollector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple tuple : tupleWindow.get()){
            String key = tuple.getStringByField("word");
            int chosenTask = chooseWorker(Math.abs(key.hashCode()), collector);
            collector.emit(tuple, new Values(chosenTask, key));
            collector.ack(tuple);
        }
    }

    private int chooseWorker(int key, OutputCollector collector){
        int keyId = key;
        int ts = numOfBatch * batchSize + numTuplesInBatch;
        numTuplesInBatch++;

        boolean isHot = cbandit.isHot(keyId, ts);

        cbandit.expireState(ts, isHot); // here

        int worker = cbandit.partition(keyId, isHot);

        cbandit.updateState(keyId, worker);
        cbandit.updateQtable(keyId, isHot, worker);
        return worker;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("chosen_task", "word"));
    }
}
