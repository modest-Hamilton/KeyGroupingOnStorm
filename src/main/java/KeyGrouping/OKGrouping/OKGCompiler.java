package KeyGrouping.OKGrouping;

import KeyGrouping.DKGrouping_string.builder.CWHashFunction;
import KeyGrouping.DKGrouping_string.builder.SpaceSaving;
import Util.Conf;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class OKGCompiler {
    private static OKGCompiler instance;
    private HashMap<String, Integer> routeTable;
    private ArrayList<String> upstreamData;
    private final long[] historicalLoad;
    private OKGCompiler() {
        this.historicalLoad = new long[Conf.DOWNSTREAMSERVER];
        this.routeTable = new HashMap<>();
        this.upstreamData = new ArrayList<>();
    }

    public static synchronized OKGCompiler getInstance() {
        if (instance == null) {
            instance = new OKGCompiler();
        }
        return instance;
    }

    public synchronized void getData(ArrayList<String> data) {
        upstreamData.addAll(data);
    }

    public void processData() {
        double theta = 0.01;
        double epsilon = theta / 2;
        SpaceSaving<String> spaceSaving = new SpaceSaving<>(epsilon,theta);

        int factor = 1;
        int codomain = (int) Math.ceil(Conf.DOWNSTREAMSERVER * factor);

        RandomDataGenerator uniformGenerator = new RandomDataGenerator();
        uniformGenerator.reSeed(1000);

        long prime = 10000019;
        long a = uniformGenerator.nextLong(1, prime - 1);
        long b = uniformGenerator.nextLong(1, prime - 1);

        CWHashFunction hashFunction = new CWHashFunction(codomain, prime, a, b);
        double[] hashFunctionDistrib = new double[codomain];

        routeTable.clear();

        for(String key:upstreamData) {
            spaceSaving.newSample(key);
            int chosen = hashFunction.hash(key);
            hashFunctionDistrib[chosen]++;
            routeTable.put(key, chosen);
        }

        HashMap<String, Integer> heavyHitter = spaceSaving.getHeavyHitters();

        for(Map.Entry<String, Integer> entry : heavyHitter.entrySet()) {
            hashFunctionDistrib[hashFunction.hash(entry.getKey())] -= entry.getValue();
        }

        for(int i = 0;i < Conf.DOWNSTREAMSERVER;i++) {
            historicalLoad[i] += hashFunctionDistrib[i];
        }

        ArrayList<Pair> keysList = new ArrayList<>();
        for(Map.Entry<String, Integer> entry : heavyHitter.entrySet()) {
            keysList.add(new Pair(entry.getKey(), entry.getValue()));
        }
        Collections.sort(keysList, new Comparator<Pair>() {
            @Override
            public int compare(Pair o1, Pair o2) {
                if(o1.load - o2.load > 0) {
                    return -1;
                }else if(o1.load - o2.load < 0) {
                    return 1;
                }else {
                    return 0;
                }
            }
        });

        for(Map.Entry<String, Integer> entry : heavyHitter.entrySet()) {
            int chosen = findLeastLoad();
            routeTable.put(entry.getKey(), chosen);
            historicalLoad[chosen] += entry.getValue();
        }
        upstreamData.clear();
    }

    private int findLeastLoad() {
        int min = 0;
        for(int i = 1;i < Conf.DOWNSTREAMSERVER;i++) {
            if(historicalLoad[i] < historicalLoad[min]) {
                min = i;
            }
        }
        return min;
    }

    private class Pair {
        String key;
        double load;

        public Pair(String key, double load) {
            this.key = key;
            this.load = load;
        }
    }

    public synchronized HashMap<String, Integer> getRouteTable() {
        return this.routeTable;
    }

}
