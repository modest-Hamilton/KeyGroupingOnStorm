package KeyGrouping.Dalton;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class DanltonGrouping implements CustomStreamGrouping {
    private ContextualBandits cbandit;
    private int numOfBatch;
    private int batchSize;
    List<Tuple> batch = new ArrayList<>();
    private int numTuplesInBatch;
    private List<Integer> targetTasks;
    public DanltonGrouping(int parallelism, int slide, int size, int numOfKeys, int batchSize){
        cbandit = new ContextualBandits(parallelism, slide, size, numOfKeys);
        this.batchSize = batchSize;
    }

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        numTuplesInBatch = 0;
        numOfBatch = 0;
        this.targetTasks = list;
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        List<Integer> boltIds = new ArrayList<>();
        int key = Math.abs(list.get(0).toString().hashCode());
        int ts = numOfBatch * batchSize + numTuplesInBatch;
        numTuplesInBatch++;

        boolean isHot = cbandit.isHot(key, ts);

        cbandit.expireState(ts, isHot); // here

        int worker = cbandit.partition(key, isHot);

        cbandit.updateState(key, worker);
        cbandit.updateQtable(key, isHot, worker);
        boltIds.add(targetTasks.get(worker));
        return boltIds;
    }
}
