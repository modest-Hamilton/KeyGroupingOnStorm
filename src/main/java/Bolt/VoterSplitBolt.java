package Bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class VoterSplitBolt extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    //0 - "county_id" 1 - "county_desc" 2 - "voter_reg_num" 3 - "ncid" 4 - "last_name" 5 - "first_name" 6 - "middle_name" 7 - "name_suffix_lbl" 8 - "status_cd" 9 - "voter_status_desc"
    //10 - "reason_cd" 11 - "voter_status_reason_desc" 12 - "res_street_address" 13 - "res_city_desc" 14 - "state_cd" 15 - "zip_code" 16 - "mail_addr1" 17 - "mail_addr2" 18 - "mail_addr3" 19 - "mail_addr4"
    //20 - "mail_city" 21 - "mail_state" 22 - "mail_zipcode" 23 - "full_phone_number" 24 - "confidential_ind" 25 - "registr_dt" 26 - "race_code" 27 - "ethnic_code" 28 - "party_cd" 29 - "gender_code"
    //30 - "birth_year" 31 - "age_at_year_end" 32 - "birth_state" 33 - "drivers_lic" 34 - "precinct_abbrv" 35 - "precinct_desc" 36 - "municipality_abbrv" 37 - "municipality_desc" 38 - "ward_abbrv" 39 - "ward_desc"
    //40 - "cong_dist_abbrv" 41 - "super_court_abbrv" 42 - "judic_dist_abbrv" 43 - "nc_senate_abbrv" 44 - "nc_house_abbrv" 45 - "county_commiss_abbrv" 46 - "county_commiss_desc" 47 - "township_abbrv" 48 - "township_desc" 49 - "school_dist_abbrv"
    //50 - "school_dist_desc" 51 - "fire_dist_abbrv" 52 - "fire_dist_desc" 53 - "water_dist_abbrv" 54 - "water_dist_desc" 55 - "sewer_dist_abbrv" 56 - "sewer_dist_desc" 57 - "sanit_dist_abbrv" 58 - "sanit_dist_desc" 59 - "rescue_dist_abbrv"
    //60 - "rescue_dist_desc" 61 - "munic_dist_abbrv" 62 - "munic_dist_desc" 63 - "dist_1_abbrv" 64 - "dist_1_desc" 65 - "vtd_abbrv" 66 - "vtd_desc"

    @Override
    public void execute(Tuple tuple) {
        String line = tuple.getStringByField("value");
        if(line.length() == 0) {
            return;
        }
        String[] voter = line.split("\t");
        if(voter.length == 0) {
            return;
        }
        if(voter[0].equals("\"county_id\"")) {
            return;
        }
        for (int i = 0; i < voter.length; i++) {
            voter[i] = voter[i].trim().replaceAll("^\"|\"$", "");
        }
        if(voter[15].equals("")) {
            voter[15] = "99999";
        }
        collector.emit(tuple, new Values(
                voter[15],
                voter[0],
                voter[4],
                voter[5],
                voter[6],
                voter[23],
                voter[28],
                voter[29],
                voter[30],
                voter[31],
                voter[32],
                voter[59],
                voter[60],
                voter[61],
                voter[62],
                voter[63],
                voter[64],
                voter[65],
                voter[66]
        ));
        collector.ack(tuple);
    }
    //28 - "party_cd" 29 - "gender_code" 30 - "birth_year" 31 - "age_at_year_end" 32 - "birth_state"
    //59 - "rescue_dist_abbrv" 60 - "rescue_dist_desc" 61 - "munic_dist_abbrv" 62 - "munic_dist_desc" 63 - "dist_1_abbrv" 64 - "dist_1_desc" 65 - "vtd_abbrv" 66 - "vtd_desc"
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
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
