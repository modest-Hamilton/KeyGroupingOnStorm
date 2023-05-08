package Bolt;

import Util.Conf;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
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
        long inTime = getCurTime();
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

    private long getCurTime() {
        try (DatagramSocket socket = new DatagramSocket()) {
            InetAddress address = InetAddress.getByName(Conf.NTP_SERVER);
            byte[] data = createMessage();
            DatagramPacket packet = new DatagramPacket(data, data.length, address, Conf.NTP_PORT);
            socket.send(packet);
            socket.receive(packet);
            long receiveTime = System.currentTimeMillis();
            byte[] responseData = packet.getData();
            long originTime = getTime(responseData, 24);
            long receiveTimeStamp = getTime(responseData, 32);
            long transmitTimeStamp = getTime(responseData, 40);
            long destinationTime = receiveTime;
            long roundTripTime = (destinationTime - originTime) - (transmitTimeStamp - receiveTimeStamp);
            long currentTime = transmitTimeStamp + roundTripTime / 2;
            return currentTime;
        } catch (Exception e) {
            System.err.println(e);
        }
        return -100000L;
    }

    private static long getTime(byte[] data, int offset) {
        ByteBuffer buffer = (ByteBuffer) ByteBuffer.allocate(Long.BYTES).put(data, offset, Long.BYTES).flip();
        long seconds = buffer.getLong();
        buffer.clear();
        return seconds == 0L ? -1L : (seconds - 2208988800L) * 1000L;
    }

    private static byte[] createMessage() {
        byte[] message = new byte[48];
        message[0] = 0x1B;
        return message;
    }
}
