package Bolt;

import Util.Conf;
import Util.FrequencyEstimate.FrequencyException;
import Util.FrequencyEstimate.LossyCounting;
import Util.cardinality.Hash;
import Util.cardinality.MurmurHash;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import redis.clients.jedis.Jedis;

import java.util.*;

public class CCGReviewBolt extends BaseWindowedBolt {
    private static final float DEFAULT_DELTA = 0.000002f; // 10^-6
    private int numServers;
    private float delta;
    private double error;  // lossy counting error
    private double varpesilon; // default = 2.0, varpesilon = ((1 + sqrt(5)) / 2) ^ (1 / z), z is the skewness of zipf data
    private Hash hash;
    private LossyCounting<Object> lossyCounting;
    private HashMap<Integer, Long> boltWeight;
    private Timer timer;
    private Jedis jedis;
    private Multimap<Object, Integer> Vk;   // routing table for heavy hitters
    private HashMap<Object, Double> Vf;   // frequency of heavy hitters
    private List<Integer> targetTasks;
    private OutputCollector collector;

    public CCGReviewBolt(int parallelism) {
        this.numServers = parallelism;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.delta = DEFAULT_DELTA;
        this.error = delta;
        this.varpesilon = 1.3;
        this.boltWeight = new HashMap<>();
        this.jedis = new Jedis(Conf.REDIS_HOST, Conf.REDIS_PORT);
        this.timer = new Timer();
//        timer.schedule(new updateWeight(), 5000,5000);

        hash = MurmurHash.getInstance();
        lossyCounting = new LossyCounting<>(error);

        Vk = HashMultimap.create();
        Vf = new HashMap<>();

        for(int i = 0;i < numServers;i++) {
            boltWeight.put(i, 0l);
        }
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        List<Tuple> Tuples = tupleWindow.get();
        List<Integer> route = new ArrayList<>();
        for(Tuple tuple: tupleWindow.get()) {
            String key = tuple.getStringByField("product_id");
            route.add(chooseWorker(key));
        }
        for (int i = 0;i < Tuples.size();i++){
            Tuple tuple = Tuples.get(i);
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

            collector.emit(tuple, new Values(route.get(i),
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
    }

    private int chooseWorker(String key) {
        int selected;

        try {
            lossyCounting.add(key);
        } catch (FrequencyException e) {
            e.printStackTrace();
        }

        long estimatedCount = lossyCounting.estimateCount(key);

        double estimatedFrequency = (float) estimatedCount / lossyCounting.size();

        if (estimatedFrequency <= delta) {
            selected = hash(key);
            Vk.put(key, selected);
        } else {
            if(!Vf.containsKey(key)) {
                selected = findLeastLoadOneInV();
                Vk.put(key, selected);
                Vf.put(key, (double)delta);
            } else if(estimatedFrequency >= Vf.get(key) * varpesilon) {
                selected = extendOneMoreNode(key);
                Vk.put(key, selected);
                Vf.replace(key, Vf.get(key) * varpesilon);
            } else {
                selected = findLeastLoadOneInVk(key);
            }
        }
        boltWeight.put(selected, boltWeight.get(selected) + 1);
        return selected;
    }

    private int extendOneMoreNode(Object key) {
        if(Vk.get(key).size() == numServers) {
            return findLeastLoadOneInV();
        }
        int[] choices = new int[numServers];
        Collection<Integer> values = Vk.get(key);
        Iterator<Integer> it = values.iterator();
        while(it.hasNext()) {
            choices[it.next()] = 1;
        }
        int res = -1;
        long minLoad = Long.MAX_VALUE;
        for(int i = 0;i < numServers;i++) {
            if(choices[i] == 1) continue;
            if(boltWeight.get(i) < minLoad) {
                res = i;
                minLoad = boltWeight.get(i);
            }
        }
        if(res == -1) {
            return findLeastLoadOneInV();
        }
        return res;
    }

    private int findLeastLoadOneInV() {
        int min = 0;
        for (int i = 1; i < numServers; i++) {
            if (boltWeight.get(i) < boltWeight.get(min)) {
                min = i;
            }
        }
        return min;
    }

    private int findLeastLoadOneInVk(Object x) {
        int min = -1;
        long minOne = Integer.MAX_VALUE;
        Collection<Integer> values = Vk.get(x);
        if (values.isEmpty()) {
            int hashed = hash(x);
            Vk.put(x, hashed);
            return hashed;
        }
        Iterator<Integer> it = values.iterator();
        int i;
        while (it.hasNext()) {
            i = it.next();
            if (boltWeight.get(i) < minOne) {
                minOne = boltWeight.get(i);
                min = i;
            }
        }
        return min;
    }

    private int hash(Object key) {
        return Math.abs(hash.hash(key)) % numServers;
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
