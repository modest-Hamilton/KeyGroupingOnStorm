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

import javax.servlet.http.HttpSessionActivationListener;
import java.util.*;

public class CCGVoteBolt extends BaseWindowedBolt {
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

    public CCGVoteBolt(int parallelism) {
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
            String key = tuple.getStringByField("zip_code");
            route.add(chooseWorker(key));
        }
        for (int i = 0;i < Tuples.size();i++){
            Tuple tuple = Tuples.get(i);
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

            collector.emit(tuple, new Values(route.get(i),
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
