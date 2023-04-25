package KeyGrouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;
import util.Seed;

import java.util.ArrayList;
import java.util.List;


public class PKGrouping implements CustomStreamGrouping {

    private long[] load;
    private HashFunction[] hashes;
    private Seed seed;
    private int numServers;
    private List<Integer> targetTasks;


    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
        this.numServers = list.size();
        this.load = new long[numServers];
        this.hashes = new HashFunction[numServers];
        this.seed = new Seed(numServers);

        hashes[0] = Hashing.murmur3_128(seed.getSeed(0));
        hashes[1] = Hashing.murmur3_128(seed.getSeed(1));

    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        String key = list.get(0).toString();
        int opt1 = Math.abs(hashes[0].hashBytes(key.getBytes()).asInt() % numServers);
        int opt2 = Math.abs(hashes[1].hashBytes(key.getBytes()).asInt() % numServers);
        int selected = load[opt1] < load[opt2] ? opt1:opt2;
        load[selected]++;
        boltIds.add(targetTasks.get(selected));
        return boltIds;
    }
}
