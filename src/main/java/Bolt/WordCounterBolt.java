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
    private double totalProcessTime;
    private long totalProcessTuple;
    private long processTuple;
    private Timer timer;
    private Timer gtimer;
    private int boltID;
    private Jedis jedis;
    private boolean stop;
    private int ticks;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.jedis = new Jedis(Conf.REDIS_HOST, Conf.REDIS_PORT);
        this.boltID = context.getThisTaskId();
        this.outputCollector = collector;
        this.outTime = 0l;
        this.totalProcessTime = 0l;
        this.totalProcessTuple = 0l;
        this.processTuple = 0l;
        this.stop = false;
        this.timer = new Timer();
        this.gtimer = new Timer();
        this.ticks = 0;

        timer.schedule(new update(), 60 * 1000, 60 * 1000);
        gtimer.schedule(new stop(), 10 * 60 * 1000, 10 * 60 * 1000);
    }

    @Override
    public void execute(Tuple tuple) {
        if(stop) {
            return;
        }
        long inTime = tuple.getLongByField("inTime");
        String word = tuple.getStringByField("word");
        if (!word.isEmpty()) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            totalProcessTuple++;
            processTuple++;
            counts.put(word, count);

            int a = 1000,b = 5000;
            for(int i = 1;i < 1000000;i++) {
                a += b * i;
                b = a * i;
                if(a > b) {
                    a = b;
                } else {
                    b -= a;
                }
            }

            outTime = System.currentTimeMillis();
            totalProcessTime += outTime - inTime;
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
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID);
    }

    private class update extends TimerTask {

        @Override
        public void run() {
//            jedis.set(String.valueOf(boltID), String.valueOf(processTuple));
//            if(processTuple != 0) {
//                System.out.println("WordCounter Bolt " + boltID + " -- processTuple:" + processTuple);
//            }
//            processTuple = 0;
//            processTime = 0;
            ticks++;
            System.out.println(ticks + " --- WordCounter Bolt " + boltID + " process Tuples: " + totalProcessTuple + " averageProcessTime: " + totalProcessTime / totalProcessTuple + " ms");
            totalProcessTime = 0;
            totalProcessTuple = 0;
        }
    }

    private class stop extends TimerTask {

        @Override
        public void run() {
            stop = true;
            timer.cancel();
        }
    }
}
