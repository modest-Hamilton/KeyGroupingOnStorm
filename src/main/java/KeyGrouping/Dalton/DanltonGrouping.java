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

    public DanltonGrouping(int parallelism, int slide, int size, int numOfKeys, int batchSize){
        cbandit = new ContextualBandits(parallelism, slide, size, numOfKeys);
        numOfBatch = 0;
        this.batchSize = batchSize;
        numTuplesInBatch = 0;
    }

    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {

    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {

        return null;
    }
}
