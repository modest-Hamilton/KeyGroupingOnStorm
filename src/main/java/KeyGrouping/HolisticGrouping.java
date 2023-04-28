package KeyGrouping;

import Util.FrequencyEstimate.FrequencyException;
import Util.FrequencyEstimate.LossyCounting;
import Util.cardinality.Hash;
import Util.cardinality.MurmurHash;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.*;

public class HolisticGrouping implements CustomStreamGrouping {
    private List<Integer> targetTasks;
    private static final float DEFAULT_BETA = 0.01f;
    private static final float DEFAULT_DELTA = 0.000001f; // 10^-6
    private int numServers;
    private float delta;
    private double error;  // lossy counting error
    private float beta;  // default = 0.01f

    private Hash hash;

    private LossyCounting<String> lossyCounting;

    private long[] localLoad;               // record downstream load
    private Multimap<Object, Integer> Vk;   // routing table for heavy hitters
    private HashMap<String, Double> Vf;   // frequency of heavy hitters
    private long estimatedCount;
    private double estimatedFrequency;
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
        this.numServers = list.size();
        this.delta = DEFAULT_DELTA;
        this.error = delta * 0.1;
        this.beta = DEFAULT_BETA;

        localLoad = new long[numServers];

        hash = MurmurHash.getInstance();
        lossyCounting = new LossyCounting<>(error);

        Vk = HashMultimap.create();
        Vf = new HashMap<String, Double>();
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

        estimatedCount = lossyCounting.estimateCount(key);

        estimatedFrequency = (float) estimatedCount / lossyCounting.size();

        if (estimatedFrequency <= delta) {
            selected = hash(key);
        } else {
            float RIm = updateRegionalLoadImbalance(key);
            if (RIm <= beta) {
                selected = findLeastLoadOneInVk(key);
            } else {
                selected = findLeastLoadOneInV();
                Vk.put(key, selected);
            }
        }

        localLoad[selected]++;
        boltIds.add(targetTasks.get(selected));
        return boltIds;
    }

    private float updateRegionalLoadImbalance(Object x) {
        float averageLoad = (lossyCounting.size() - 1) / (float) numServers;
        return averageLoad == 0 ? 0.0f : (getRegionalLoad(x) - averageLoad) / averageLoad;
    }

    private long getRegionalLoad(Object x) {
        Collection<Integer> values = Vk.get(x);
        if (values.isEmpty()) {
            return localLoad[hash(x)];
        }
        Iterator<Integer> it = values.iterator();
        long regionalLoad = 0;
        while (it.hasNext()) {
            regionalLoad += localLoad[it.next()];
        }
        return regionalLoad / values.size();
    }

    private int findLeastLoadOneInV() {
        int min = 0;
        for (int i = 1; i < numServers; i++) {
            if (localLoad[i] < localLoad[min]) {
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
            if (localLoad[i] < minOne) {
                minOne = localLoad[i];
                min = i;
            }
        }
        return min;
    }

    private int hash(Object key) {
        return Math.abs(hash.hash(key)) % numServers;
    }
}
