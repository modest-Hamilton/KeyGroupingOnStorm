package KeyGrouping.PStreamForReview.bolt;

import KeyGrouping.PStreamForReview.Constraints;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SchedulerBolt extends BaseRichBolt {
    private String UPSTREAM_COMPONENT_ID;
    private String UPSTREAM_FIELDS;
    private OutputCollector collector;
    private CountingBloomFilter bf;

    public SchedulerBolt(String UPSTREAM_COMPONENT_ID,String UPSTREAM_FIELDS) {
        this.UPSTREAM_COMPONENT_ID = UPSTREAM_COMPONENT_ID;
        this.UPSTREAM_FIELDS=UPSTREAM_FIELDS;
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.bf = new CountingBloomFilter(16,4,1);
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

    public void execute(Tuple tuple) {
        if(tuple.getSourceComponent().equals(UPSTREAM_COMPONENT_ID)){
            long inTime = tuple.getLongByField("inTime");
            String product_id = tuple.getStringByField(UPSTREAM_FIELDS);
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

            if(product_id.length() <= 0) {
                collector.ack(tuple);
                return;
            }
            collector.emit(Constraints.coinFileds, new Values(product_id));
            Key ky = new Key(product_id.getBytes());
            if(bf.membershipTest(ky))
                collector.emit(Constraints.hotFileds, tuple, new Values(
                        product_id,
                        inTime,
                        marketplace,
                        customer_id ,
                        review_id ,
                        product_parent,
                        product_title,
                        product_category,
                        star_rating,
                        helpful_votes,
                        total_votes,
                        vine ,
                        verified_purchase,
                        review_headline,
                        review_body ,
                        review_date
                ));
            else
                collector.emit(Constraints.nohotFileds, tuple, new Values(
                        product_id,
                        inTime,
                        marketplace,
                        customer_id ,
                        review_id ,
                        product_parent,
                        product_title,
                        product_category,
                        star_rating,
                        helpful_votes,
                        total_votes,
                        vine ,
                        verified_purchase,
                        review_headline,
                        review_body ,
                        review_date
                ));

        }else {
            String key = tuple.getStringByField("product_id");
            Integer type = tuple.getIntegerByField(Constraints.typeFileds);
            Key hk = new Key(key.getBytes());
            if(!bf.membershipTest(hk) && type.equals(1))
                bf.add(hk);
            if(bf.membershipTest(hk) && type.equals(0))
                bf.delete(hk);
        }
//        collector.ack(tuple);
    }

    public void cleanup(){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constraints.coinFileds, new Fields("product_id"));
        declarer.declareStream(Constraints.hotFileds, new Fields(
                "product_id",
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
                "review_date"
        ));
        declarer.declareStream(Constraints.nohotFileds, new Fields(
                "product_id",
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
                "review_date"
        ));
    }

}
