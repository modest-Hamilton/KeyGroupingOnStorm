package Bolt;

import javafx.util.Pair;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

public class ReviewProcessBolt extends BaseRichBolt {
    private OutputCollector outputCollector;
    private long outTime;
    private double totalProcessTime;
    private long totalProcessTuple;
    private long processTuple;
    private Timer timer;
    private Timer gtimer;
    private int boltID;
    private boolean stop;
    private int ticks;
    private HashMap<String, Long> ratingOfProduct;
    private HashMap<String, Long> counts;
    private HashMap<String, List<Pair<String,String>>> customerInfo;
    private volatile static int nothing = 0;
    private boolean enableLog;
    private FileHandler handler;
    private Logger LOGGER;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.boltID = topologyContext.getThisTaskId();
        this.outputCollector = outputCollector;
        this.outTime = 0l;
        this.totalProcessTime = 0l;
        this.totalProcessTuple = 0l;
        this.processTuple = 0l;
        this.stop = false;
        this.enableLog = false;
        this.timer = new Timer();
        this.gtimer = new Timer();
        this.ticks = 0;
        this.counts = new HashMap<>();
        this.ratingOfProduct = new HashMap<>();
        this.customerInfo = new HashMap<>();

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
//        gtimer.schedule(new stop(), 10 * 60 * 1000, 10 * 60 * 1000);
    }

    /*
        Fields("product_id",
           "inTime",
           "marketplace",
           "customer_id",
           "review_id",
           "product_parent",
           "product_title",
           "product_category",
           "star_rating",
           "helpful_votes",
           "total_votes",
           "vine",
           "verified_purchase",
           "review_headline",
           "review_body",
           "review_date"));
     */
    @Override
    public void execute(Tuple tuple) {
        String product_id = tuple.getStringByField("product_id");
        String marketplace = tuple.getStringByField("marketplace");
        String customer_id = tuple.getStringByField("customer_id");
        String review_id = tuple.getStringByField("review_id");
        String product_parent = tuple.getStringByField("product_parent");
        String product_title = tuple.getStringByField("product_title");
        String product_category = tuple.getStringByField("product_category");
        String star_rating = tuple.getStringByField("star_rating");
        String helpful_votes = tuple.getStringByField("helpful_votes");
        String total_votes = tuple.getStringByField("total_votes");
        String vine = tuple.getStringByField("vine");
        String verified_purchase = tuple.getStringByField("verified_purchase");
        String review_headline = tuple.getStringByField("review_headline");
        String review_body = tuple.getStringByField("review_body");
        String review_date = tuple.getStringByField("review_date");
        long inTime = tuple.getLongByField("inTime");
        totalProcessTuple++;

        long rating = Integer.valueOf(star_rating) * (Integer.valueOf(helpful_votes) + Integer.valueOf(total_votes));
        if(ratingOfProduct.containsKey(product_id)) {
            ratingOfProduct.put(product_id, ratingOfProduct.get(product_id) + rating);
        } else {
            ratingOfProduct.put(product_id, rating);
        }

        if(customerInfo.containsKey(customer_id)) {
            customerInfo.get(customer_id).add(new Pair<>(product_id,review_id));
        } else {
            List<Pair<String,String>> info = new ArrayList<>();
            info.add(new Pair<>(product_id,review_id));
            customerInfo.put(customer_id,info);
        }

        for(String word:review_body.split(" ")) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
        }

        for(String word:review_headline.split(" ")) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
        }

        for(String word:product_title.split(" ")) {
            Long count = counts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;
            counts.put(word, count);
        }

        nothing = 0;
        while (nothing < 100000) {
            doNothing();
            ++nothing;
        }

        outTime = System.currentTimeMillis();
        totalProcessTime += (outTime - inTime);
        outputCollector.emit(tuple,new Values(product_id,star_rating));
    }

    private void doNothing() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private class update extends TimerTask {

        @Override
        public void run() {
            ticks++;
            if(enableLog) {
                LOGGER.log(Level.INFO,ticks + " --- ReviewProcess Bolt " + boltID + " process Tuples: " + totalProcessTuple + " averageProcessTime: " + totalProcessTime / totalProcessTuple + " ms");
            } else {
                System.out.println(ticks + " --- ReviewProcess Bolt " + boltID + " process Tuples: " + totalProcessTuple + " averageProcessTime: " + totalProcessTime / totalProcessTuple + " ms");
            }
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
