package Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.*;

public class ZipfDataCounterBolt extends BaseRichBolt {
    private Map<String, Long> counts;
    private OutputCollector collector;
    private long totalProcessTuple;
    private double totalProcessTime;
    private int ticks;
    private boolean enableLog;
    private FileHandler handler;
    private Logger LOGGER;
    private Timer timer;
    private int boltID;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<>();
        this.ticks = 0;
        this.timer = new Timer();
        this.boltID = topologyContext.getThisTaskId();
        this.enableLog = false;

        if(this.enableLog) {
            this.LOGGER = Logger.getLogger(String.valueOf(boltID));
            String logFileName = "/log/Bolt-" + String.valueOf(boltID) + ".log";
            try {
                this.handler = new FileHandler(logFileName, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            handler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    String message = record.getMessage();
                    if (message.endsWith(System.lineSeparator())) {
                        message = message.substring(0, message.length() - System.lineSeparator().length());
                    }
                    return "[" + record.getLevel() + "] " + message + System.lineSeparator();
                }
            });
            LOGGER.addHandler(handler);
        }

        timer.schedule(new update(), 60 * 1000, 60 * 1000);
    }

    @Override
    public void execute(Tuple tuple) {
        String num = tuple.getStringByField("num");
        Long count = counts.get(num);
        if (count == null) {
            count = 0L;
        }
        count++;
        totalProcessTuple++;
        counts.put(num, count);
        collector.emit(tuple, new Values(num));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private class update extends TimerTask {

        @Override
        public void run() {
            ticks++;
            if(enableLog) {
                LOGGER.log(Level.INFO,ticks + " --- WordCounter Bolt " + boltID + " process Tuples: " + totalProcessTuple + " averageProcessTime: " + totalProcessTime / totalProcessTuple + " ms");
            } else {
                System.out.println(ticks + " --- WordCounter Bolt " + boltID + " process Tuples: " + totalProcessTuple + " averageProcessTime: " + totalProcessTime / totalProcessTuple + " ms");
            }
            totalProcessTime = 0;
            totalProcessTuple = 0;
        }
    }
}
