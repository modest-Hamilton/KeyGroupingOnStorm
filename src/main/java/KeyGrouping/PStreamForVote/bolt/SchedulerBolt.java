package KeyGrouping.PStreamForVote.bolt;

import KeyGrouping.PStreamForVote.Constraints;
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
            String zip_code = tuple.getStringByField(UPSTREAM_FIELDS);
            String county_id = tuple.getStringByField("county_id");
            String last_name = tuple.getStringByField("last_name");
            String first_name = tuple.getStringByField("first_name");
            String middle_name = tuple.getStringByField("middle_name");
            String full_phone_number = tuple.getStringByField("full_phone_number");
            String party_cd = tuple.getStringByField("party_cd");
            String gender_code = tuple.getStringByField("gender_code");
            String birth_year = tuple.getStringByField("birth_year");
            String age_at_year_end = tuple.getStringByField("age_at_year_end");
            String birth_state = tuple.getStringByField("birth_state");
            String rescue_dist_abbrv = tuple.getStringByField("rescue_dist_abbrv");
            String rescue_dist_desc = tuple.getStringByField("rescue_dist_desc");
            String munic_dist_abbrv = tuple.getStringByField("munic_dist_abbrv");
            String munic_dist_desc = tuple.getStringByField("munic_dist_desc");
            String dist_1_abbrv = tuple.getStringByField("dist_1_abbrv");
            String dist_1_desc = tuple.getStringByField("dist_1_desc");
            String vtd_abbrv = tuple.getStringByField("vtd_abbrv");
            String vtd_desc = tuple.getStringByField("vtd_desc");

            if(zip_code.length() <= 0) {
                collector.ack(tuple);
                return;
            }
            collector.emit(Constraints.coinFileds, new Values(zip_code));
            Key ky = new Key(zip_code.getBytes());
            if(bf.membershipTest(ky))
                collector.emit(Constraints.hotFileds, tuple, new Values(
                        zip_code,
                        county_id,
                        last_name,
                        first_name,
                        middle_name,
                        full_phone_number,
                        party_cd,
                        gender_code,
                        birth_year,
                        age_at_year_end,
                        birth_state,
                        rescue_dist_abbrv,
                        rescue_dist_desc,
                        munic_dist_abbrv,
                        munic_dist_desc,
                        dist_1_abbrv,
                        dist_1_desc,
                        vtd_abbrv,
                        vtd_desc
                ));
            else
                collector.emit(Constraints.nohotFileds, tuple, new Values(
                        zip_code,
                        county_id,
                        last_name,
                        first_name,
                        middle_name,
                        full_phone_number,
                        party_cd,
                        gender_code,
                        birth_year,
                        age_at_year_end,
                        birth_state,
                        rescue_dist_abbrv,
                        rescue_dist_desc,
                        munic_dist_abbrv,
                        munic_dist_desc,
                        dist_1_abbrv,
                        dist_1_desc,
                        vtd_abbrv,
                        vtd_desc
                ));

        }else {
            String key = tuple.getStringByField(UPSTREAM_FIELDS);
            Integer type = tuple.getIntegerByField(Constraints.typeFileds);
            Key hk = new Key(key.getBytes());
            if(!bf.membershipTest(hk) && type.equals(1))
                bf.add(hk);
            if(bf.membershipTest(hk) && type.equals(0))
                bf.delete(hk);
        }
        collector.ack(tuple);
    }

    public void cleanup(){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(Constraints.coinFileds, new Fields("zip_code"));
        declarer.declareStream(Constraints.hotFileds, new Fields(
                "zip_code",
                "county_id",
                "last_name",
                "first_name",
                "middle_name",
                "full_phone_number",
                "party_cd",
                "gender_code",
                "birth_year",
                "age_at_year_end",
                "birth_state",
                "rescue_dist_abbrv",
                "rescue_dist_desc",
                "munic_dist_abbrv",
                "munic_dist_desc",
                "dist_1_abbrv",
                "dist_1_desc",
                "vtd_abbrv",
                "vtd_desc"
        ));
        declarer.declareStream(Constraints.nohotFileds, new Fields(
                "zip_code",
                "county_id",
                "last_name",
                "first_name",
                "middle_name",
                "full_phone_number",
                "party_cd",
                "gender_code",
                "birth_year",
                "age_at_year_end",
                "birth_state",
                "rescue_dist_abbrv",
                "rescue_dist_desc",
                "munic_dist_abbrv",
                "munic_dist_desc",
                "dist_1_abbrv",
                "dist_1_desc",
                "vtd_abbrv",
                "vtd_desc"
        ));
    }

}
