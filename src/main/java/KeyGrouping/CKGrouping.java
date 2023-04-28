package KeyGrouping;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.List;

public class CKGrouping implements CustomStreamGrouping {
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        return null;
    }
}
