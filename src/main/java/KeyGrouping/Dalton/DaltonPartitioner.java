
package KeyGrouping.Dalton;

import KeyGrouping.Dalton.prompt.Prompt;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import java.util.ArrayList;
import java.util.List;

public class DaltonPartitioner extends BaseAggregator<Integer> {

    private ContextualBandits cbandit;
    private int numOfBatch;
    private int batchSize;
    List<Prompt.Tuple> batch = new ArrayList<>();
    private int numTuplesInBatch;

    public DaltonPartitioner(int parallelism, int slide, int size, int numOfKeys, int batchSize){
        cbandit = new ContextualBandits(parallelism, slide, size, numOfKeys);
        numOfBatch = 0;
        this.batchSize = batchSize;
        numTuplesInBatch = 0;
    }

    @Override
    public Integer init (Object batchId, TridentCollector collector){
        return 0;
    }

    @Override
    public void aggregate(Integer state, TridentTuple tuple, TridentCollector collector){
        int keyId = tuple.getInteger(0);
        batch.add(new Prompt.Tuple(keyId, tuple.getString(1)));
    }

    @Override
    public void complete(Integer state, TridentCollector tridentCollector) {
        for (Prompt.Tuple tuple : batch){
            chooseWorker(tuple, tridentCollector);
        }
        batch.clear();

        numOfBatch++;
        numTuplesInBatch = 0;
    }

    private void chooseWorker(Prompt.Tuple tuple, TridentCollector collector){
        int keyId = tuple.key;
        int ts = numOfBatch * batchSize + numTuplesInBatch;
        numTuplesInBatch++;

        boolean isHot = cbandit.isHot(keyId, ts);

        cbandit.expireState(ts, isHot); // here

        int worker = cbandit.partition(keyId, isHot);

        collector.emit(new Values(keyId, tuple.str, worker));

        cbandit.updateState(keyId, worker);
        cbandit.updateQtable(keyId, isHot, worker);
    }
}
