package KeyGrouping;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class HashGrouping implements CustomStreamGrouping {
    private List<Integer> targetTasks;
    private HashFunction hash;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
        this.hash = Hashing.murmur3_128(13);
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        String key = list.get(0).toString();
        boltIds.add(targetTasks.get(Math.abs(hash.hashBytes(key.toString().getBytes()).asInt() % targetTasks.size())));
        return boltIds;
    }
}
