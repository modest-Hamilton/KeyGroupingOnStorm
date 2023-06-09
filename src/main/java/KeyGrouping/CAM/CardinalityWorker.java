
package KeyGrouping.CAM;

import java.io.Serializable;
import java.util.BitSet;

/**
 * Class responsible for the maintenance of load statistic for the cardinality partitioners
 */
public class CardinalityWorker implements Serializable {
    private int aggrLoad;
    private BitSet batch;

    public CardinalityWorker(){
        aggrLoad = 0;
        batch = new BitSet();
    }

    public void updateState(int k){
        aggrLoad++;
        batch.set(k);
    }

    public void clear(){
        batch.clear();
        aggrLoad = 0;
    }

    public int getLoad(){
        return aggrLoad;
    }

    public int getCardinality(){
        return batch.cardinality();
    }

    public boolean hasSeen(int key){
        return batch.get(key);
    }
}
