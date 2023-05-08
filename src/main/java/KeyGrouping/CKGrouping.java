package KeyGrouping;

import Util.Conf;
import Util.FrequencyEstimate.FrequencyException;
import Util.FrequencyEstimate.LossyCounting;
import Util.cardinality.Hash;
import Util.cardinality.MurmurHash;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import redis.clients.jedis.Jedis;

import java.util.*;

public class CKGrouping implements CustomStreamGrouping {
    private static final float DEFAULT_DELTA = 0.000001f; // 10^-6
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

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
        this.numServers = list.size();
        this.delta = DEFAULT_DELTA;
        this.error = delta * 0.1;
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
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        int selected;
        String key = list.get(0).toString();

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
        boltIds.add(targetTasks.get(selected));
        return boltIds;
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

    private class updateWeight extends TimerTask {

        @Override
        public void run() {
            for(int i = 0;i < numServers;i++) {
//                System.out.println("bolt " + i + " get " + targetTasks.get(i) + " weight " + Long.valueOf(jedis.get(String.valueOf(targetTasks.get(i)))));
                boltWeight.put(i, Long.valueOf(jedis.get(String.valueOf(targetTasks.get(i)))));
            }
        }
    }
}
