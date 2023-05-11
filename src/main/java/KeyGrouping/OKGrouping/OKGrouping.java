package KeyGrouping.OKGrouping;


import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class OKGrouping implements CustomStreamGrouping {
    private List<Integer> targetTasks;

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        boltIds.add(targetTasks.get(Integer.valueOf(list.get(0).toString())));
        return boltIds;
    }
}
