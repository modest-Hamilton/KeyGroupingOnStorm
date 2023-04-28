package Bolt;

import Util.Conf;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;


public class WordCounterBolt extends BaseRichBolt {
    private Map<String, Long> counts = new HashMap<String, Long>();
    private OutputCollector outputCollector;
    private long outTime;
    private double processTime;
    private long processTuple;
    private Timer timer;
    private int boltID;
    private Jedis jedis;
    private boolean stop;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.jedis = new Jedis(Conf.REDIS_HOST, Conf.REDIS_PORT);
        this.boltID = context.getThisTaskId();
        final int thisTaskId = context.getThisTaskIndex();
        this.outputCollector = collector;
        this.outTime = 0l;
        this.processTime = 0l;
        this.processTuple = 0l;
        this.stop = false;
        this.timer = new Timer();
        timer.schedule(new update(), 4000, 5000);
    }

    @Override
    public void execute(Tuple tuple) {
        if(isTickTuple(tuple)) {
            System.out.println("WordCounter Bolt: Tick Tuple !");
            stop = true;
        }
        if(stop) {
            return;
        }
//        long ID = tuple.getLongByField("ID");
        long inTime = tuple.getLongByField("inTime");
        String word = tuple.getStringByField("word");
        if (!word.isEmpty()) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            processTuple++;
            counts.put(word, count);
            outTime = System.currentTimeMillis();
            processTime += outTime - inTime;
            outputCollector.emit(tuple,new Values(word,count));
        }
//        outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }


    @Override
    public void cleanup() {
        for(Map.Entry<String, Long> entry : counts.entrySet()) {
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID);
    }

    private class update extends TimerTask {

        @Override
        public void run() {
            jedis.set(String.valueOf(boltID), String.valueOf(processTuple));
            System.out.println("WordCounter Bolt "+ boltID + " -- processTuple:" + processTuple + "   averageProcessTime:" + processTime / processTuple + "ms");
//            processTuple = 0;
            processTime = 0;
        }
    }
}
