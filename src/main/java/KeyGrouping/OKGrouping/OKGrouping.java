package KeyGrouping.OKGrouping;


import KeyGrouping.DKGrouping_string.builder.CWHashFunction;
import KeyGrouping.DKGrouping_string.builder.SpaceSaving;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.util.ArrayList;
import java.util.List;

public class OKGrouping implements CustomStreamGrouping {

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        boltIds.add(Integer.valueOf(list.get(0).toString()));
        return boltIds;
    }
}
