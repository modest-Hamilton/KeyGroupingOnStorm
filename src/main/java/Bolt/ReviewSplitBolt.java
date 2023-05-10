package Bolt;

import Util.Conf;
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
import java.util.Map;

public class ReviewSplitBolt extends BaseRichBolt {
    private OutputCollector collector;
    NTPUDPClient timeClient;
    boolean stop;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.timeClient = new NTPUDPClient();

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
        long inTime = 0;
        try {
            inTime = getCurTime();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        collector.emit(new Values(review[3],
                                  inTime,
                                  review[0],
                                  review[1],
                                  review[2],
                                  review[4],
                                  review[5],
                                  review[6],
                                  review[7],
                                  review[8],
                                  review[9],
                                  review[10],
                                  review[11],
                                  review[12],
                                  review[13],
                                  review[14]));
        collector.ack(tuple);
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
                                                "review_date"));
    }

    private long getCurTime() throws IOException {
        InetAddress inetAddress = InetAddress.getByName(Conf.NTP_SERVER);
        TimeInfo timeInfo = timeClient.getTime(inetAddress);
        long currentTimeMillis = timeInfo.getMessage().getTransmitTimeStamp().getTime();
        return currentTimeMillis;
    }
}
