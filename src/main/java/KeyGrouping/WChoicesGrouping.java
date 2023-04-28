package KeyGrouping;


import com.clearspring.analytics.stream.Counter;
import com.google.common.hash.HashFunction;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import com.google.common.hash.Hashing;
import com.clearspring.analytics.stream.StreamSummary;
import util.Seed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class WChoicesGrouping implements CustomStreamGrouping {
    private int numServers;
    private long[] load;
    private long[] loadHH;
    private StreamSummary<String> streamSummary;
    private Seed seed;
    private HashFunction[] hashes;
    private long totalElement;
    private int threshold;
    private int DEFAULT_CHOICES = 2;
    private List<Integer> targetTasks;
    public final static int STREAM_SUMMARY_CAPACITY = 100;
    public WChoicesGrouping(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
        this.numServers = list.size();
        this.load = new long[numServers];
        this.loadHH = new long[numServers];
        this.streamSummary = new StreamSummary<>(STREAM_SUMMARY_CAPACITY);
        this.seed = new Seed(numServers);

        this.hashes = new HashFunction[numServers];
        for(int i = 0;i < hashes.length;i++) {
            hashes[i] = Hashing.murmur3_128(seed.getSeed(i));
        }
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        String key = list.get(0).toString();
        totalElement++;
        streamSummary.offer(key);

        float probability = 2 / (float) (numServers * threshold);
        HashMap<String, Long> topK = getTopK(streamSummary, probability, totalElement);

        if(topK.containsKey(key)) {
            int[] wChoices = new int[numServers];
            int j = 0;
            while(j < numServers) {
                wChoices[j] = j;
                j++;
            }
            int selected = getMinLoad(merge(load, loadHH), wChoices);
            load[selected]++;
            boltIds.add(targetTasks.get(selected));
            return boltIds;
        }

        int j = 0;
        int[] chosen = new int[DEFAULT_CHOICES];
        while(j < DEFAULT_CHOICES) {
            chosen[j] = Math.abs(hashes[j].hashBytes(key.getBytes()).asInt() % numServers);
            j++;
        }

        int selected = getMinLoad(merge(load, loadHH), chosen);
        load[selected]++;
        boltIds.add(targetTasks.get(selected));
        return boltIds;
    }

    public HashMap<String, Long> getTopK(StreamSummary<String> streamSummary, float probability, long totalItems) {
        HashMap<String, Long> topK = new HashMap<>();
        List<Counter<String>> counters = streamSummary.topK(streamSummary.getCapacity());

        for(Counter<String> counter : counters) {
            float count = counter.getCount();
            float error = counter.getError();
            float itemProb = (count + error) / totalItems;
            if(itemProb > probability) {
                topK.put(counter.getItem(), counter.getCount());
            }
        }
        return topK;
    }

    public long[] merge(long[] arr1, long[] arr2) {
        long[] result = new long[arr1.length];
        for(int i = 0;i < arr1.length;i++) {
            result[i] = arr1[i] + arr2[i];
        }
        return result;
    }

    public int getMinLoad(long[] load,int[] selected) {
        int min = selected[0];
        long minOne = load[selected[0]];
        for(int i = 0;i < selected.length;i++) {
            if(load[selected[i]] < minOne) {
                minOne = load[selected[i]];
                min = selected[i];
            }
        }
        return min;
    }

}
