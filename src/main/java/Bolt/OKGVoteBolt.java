package Bolt;

import KeyGrouping.OKGrouping.OKGCompiler;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class OKGVoteBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 22L;
    private ArrayList<String> collectKey;
    private OKGCompiler compiler;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collectKey = new ArrayList<>();
        this.collector = outputCollector;
        this.compiler = OKGCompiler.getInstance();
    }
    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple tuple:tupleWindow.get()) {
            collectKey.add(tuple.getStringByField("zip_code"));
        }
        compiler.getData(collectKey);
        compiler.processData();
        HashMap<String, Integer> routeTable = compiler.getRouteTable();
        for(Tuple tuple:tupleWindow.get()) {
            String zip_code = tuple.getStringByField("zip_code");
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

            int chosenTask = routeTable.get(zip_code);
            collector.emit(tuple, new Values(chosenTask,
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
                    vtd_desc));
            collector.ack(tuple);
        }
        collectKey.clear();
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("chosen_task",
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
                "vtd_desc"));
    }
}
