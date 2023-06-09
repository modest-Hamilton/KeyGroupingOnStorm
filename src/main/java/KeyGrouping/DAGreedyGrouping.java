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


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;


public class DAGreedyGrouping implements CustomStreamGrouping {
    private LossyCounting<Object> lossyCounting;
    private HashMap<Integer, Long> boltWeight;
    private double error;
    private Multimap<Object, Integer> Vk;   // routing table for heavy hitters
    private List<Integer> targetTasks;
    private int numServer;
    private Hash hash;
    private double delta;
    private double alpha;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
        numServer = list.size();
        this.error = 0.000002f;
        this.boltWeight = new HashMap<>();
        hash = MurmurHash.getInstance();
        lossyCounting = new LossyCounting<>(error);
        this.delta = 0.01f;
        this.alpha = 0.5f;
        Vk = HashMultimap.create();
        for(int i = 0;i < numServer;i++) {
            boltWeight.put(i, 0l);
        }
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        int selected = 0;
        String key = list.get(0).toString();

        try {
            lossyCounting.add(key);
        } catch (FrequencyException e) {
            e.printStackTrace();
        }

        long estimatedCount = lossyCounting.estimateCount(key);

        double estimatedFrequency = (float) estimatedCount / lossyCounting.size();

        if (estimatedFrequency <= delta * numServer) {
            selected = hash(key);
            Vk.put(key, selected);
        } else {
            double score = 0.0;
            for(int v = 0;v < numServer;v++) {
                double c = calc(v, key);
                int ret = Double.compare(score, c);
                if(ret <= 0) {
                    score = c;
                    selected = v;
                }
            }
        }
        boltWeight.put(selected, boltWeight.get(selected) + 1);
        boltIds.add(targetTasks.get(selected));
        return boltIds;
    }

    private double calc(int i, Object key) {
        Collection<Integer> values = Vk.get(key);
        boolean containsValue = values.contains(i);
        if(containsValue) {
            return (alpha * 1 + alpha * loadImbalance());
        } else {
            return (alpha * 0 + alpha * loadImbalance());
        }
    }

    private int hash(Object key) {
        return Math.abs(hash.hash(key)) % numServer;
    }

    private double loadImbalance() {
        long sum = 0, maximum = 0;
        for(int i = 0;i < numServer;i++) {
            if(boltWeight.containsKey(i)) {
                sum += boltWeight.get(i);
                maximum = Math.max(maximum, boltWeight.get(i));
            }
        }
        double avg = sum / numServer;
        return ((double)(maximum) - avg) / avg;
    }
}
