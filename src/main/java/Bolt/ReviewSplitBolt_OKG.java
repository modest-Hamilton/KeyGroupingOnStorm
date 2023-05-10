package Bolt;

import KeyGrouping.OKGrouping.OKGCompiler;
import Util.Conf;
import Util.pair;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


public class ReviewSplitBolt_OKG extends BaseRichBolt {
    private OutputCollector collector;
    NTPUDPClient timeClient;
    boolean stop;
    private OKGCompiler compiler;
    private ArrayList<String> collectData;
    private ArrayList<pair<Long, Tuple>> collectTuples;
    private int learningLength;
    private int boltID;
    private boolean thisRound;
    public ReviewSplitBolt_OKG(int learningLength) {

        this.learningLength = learningLength;
        this.collectTuples = new ArrayList<>();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.boltID = topologyContext.getThisTaskId();
        this.collector = outputCollector;
        this.timeClient = new NTPUDPClient();
        this.compiler = OKGCompiler.getInstance();
        this.collectData = new ArrayList<>();
        this.thisRound = false;
        this.stop = false;
    }

    @Override
    public void execute(Tuple tuple) {
        /*
            0-marketplace 1-customer_id 2-review_id	    3-product_id  4-product_parent  5-product_title 6-product_category 7-star_rating 8-helpful_votes 9-total_votes 10-vine 11-verified_purchase 12-review_headline 13-review_body 14-review_date
         */
        String line = tuple.getStringByField("value");
        if(line.length() == 0) {
            return;
        }
        String[] review = line.split("\t");
        if(review[0].equals("marketplace")) {
            return;
        }
        if(collectData.size() == learningLength) {
            if(!thisRound) {
                System.out.println("bolt" + boltID + " entered OKG process...");
                //sendData -> product_id
                compiler.getData(collectData);
                System.out.println("bolt" + boltID + " finish sendData...");
            }
            thisRound = true;
            compiler.processAsynchronously(boltID);
            System.out.println("bolt" + boltID + " finish processAsynchronously...");
            //TODO:wait wait wait wait wait wait

            if(compiler.isFinished) {
                //get routingTable <product_id, taskID>
                HashMap<String, Integer> routeTable = compiler.getRouteTable();

                //emitData
                for(pair<Long, Tuple> data:collectTuples) {
                    String rawData = data.getSecond().getStringByField("value");
                    String[] tupleData = rawData.split("\t");
                    String product_id = tupleData[3];
                    collector.emit(new Values(routeTable.get(product_id),
                            product_id,
                            data.getFirst(),
                            tupleData[0],
                            tupleData[1],
                            tupleData[2],
                            tupleData[4],
                            tupleData[5],
                            tupleData[6],
                            tupleData[7],
                            tupleData[8],
                            tupleData[9],
                            tupleData[10],
                            tupleData[11],
                            tupleData[12],
                            tupleData[13],
                            tupleData[14]));
                    collector.ack(data.getSecond());
                }
                //clear collectData
                if(compiler.allDone) {
                    collectData.clear();
                    collectTuples.clear();
                }
            }
        } else {
            long inTime = 0;
//            try {
//                inTime = getCurTime();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
            collectData.add(review[3]);
            collectTuples.add(new pair<>(inTime, tuple));
            thisRound = false;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("product_id",
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
                "review_date",
                "chosen_task"));
    }

    private long getCurTime() throws IOException {
        InetAddress inetAddress = InetAddress.getByName(Conf.NTP_SERVER);
        TimeInfo timeInfo = timeClient.getTime(inetAddress);
        long currentTimeMillis = timeInfo.getMessage().getTransmitTimeStamp().getTime();
        return currentTimeMillis;
    }

}
