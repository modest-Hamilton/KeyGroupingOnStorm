package KeyGrouping.CAM;

import java.util.ArrayList;
import java.util.List;

public class CardinalityPartitioner{
    private final double HASH_C = (Math.sqrt(5) - 1) / 2;
    private int parallelism;

    protected List<CardinalityWorker> workersStats;


    public CardinalityPartitioner(int p){
        parallelism = p;

        workersStats = new ArrayList<>();
        for(int i = 0; i < p; i++){
            CardinalityWorker w = new CardinalityWorker();
            workersStats.add(w);
        }

        for(int i = 0; i < parallelism; i++){
            workersStats.get(i).clear();
        }
    }

    protected int hash1(int n) {
        return n % parallelism;
    }

    protected int hash2(int n) {
        double a = (n + 1) * HASH_C;
        return (int)Math.floor(parallelism * (a - (int) a));
    }

    protected void updateState(int worker, int key){
        workersStats.get(worker).updateState(key);
    }
}
