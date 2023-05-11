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

public class OKGroupingBolt extends BaseWindowedBolt {
    private static final long serialVersionUID = 1L;
    private ArrayList<String> collectKey;
    private OKGCompiler compiler;
    private OutputCollector collector;
    public OKGroupingBolt() {
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collectKey = new ArrayList<>();
        this.collector = outputCollector;
        this.compiler = OKGCompiler.getInstance();
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple tuple:tupleWindow.get()) {
            collectKey.add(tuple.getStringByField("product_id"));
        }
        compiler.getData(collectKey);
        compiler.processData();
        HashMap<String, Integer> routeTable = compiler.getRouteTable();
        for(Tuple tuple:tupleWindow.get()) {
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
            int chosenTask = routeTable.get(product_id);
            collector.emit(tuple, new Values(chosenTask,
                    product_id,
                    inTime,
                    marketplace,
                    customer_id,
                    review_id,
                    product_parent,
                    product_title,
                    product_category,
                    star_rating,
                    helpful_votes,
                    total_votes,
                    vine,
                    verified_purchase,
                    review_headline,
                    review_body,
                    review_date));
            collector.ack(tuple);
        }
        collectKey.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("chosen_task",
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
                "review_date"));
    }
}
