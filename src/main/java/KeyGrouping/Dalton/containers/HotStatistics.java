

package KeyGrouping.Dalton.containers;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Class responsible for maintaining frequency statistics
 */
public class HotStatistics implements Serializable {
    private CountMinSketch countMinSketch;
    private Set<Integer> hotKeys;
    private int total;
    private Map<Integer, Integer> keysStatistics;
    private double threshold;
    private boolean usingSketch;

    private int hotInterval;
    private int nextUpdateHot;
    private int numWorkers;

    public HotStatistics(int numWorkers, int estimatedNumKeys, int hotInterval){
        countMinSketch = new CountMinSketch(0.05, 0.9, 33);
        hotKeys = new HashSet<>(numWorkers);
        total = 0;
        threshold = Double.MAX_VALUE;
        keysStatistics = new HashMap<>(estimatedNumKeys);
        usingSketch = false;

        this.hotInterval = hotInterval;
        nextUpdateHot = hotInterval;
        this.numWorkers = numWorkers;
    }

    /**
     * @param keyId the key of the newly arrived tuple
     * @return 0 if the key is not hot, 1 if the key was already hot before the arrival of the last tuple,
     * expirationTimestamp if the key just became hot after the arrival of the last tuple
     */
    private int isHotExact(int keyId){
        boolean isHot = hotKeys.contains(keyId);
        total++;
        int result = 1; // 1 means hot, 0 not hot
        int freq = Integer.MAX_VALUE;
        if (!isHot) {
            freq = keysStatistics.getOrDefault(keyId, 0) + 1;
            keysStatistics.put(keyId, freq);
            if (freq > threshold){
                hotKeys.add(keyId);
                isHot = true;
                result = nextUpdateHot + hotInterval;
            }
            else{
                result = 0;
            }
        }

        return result;
    }

    /**
     * @param keyId the key of thw newly arrived tuple
     * @return 0 if the key is not hot, 1 if the key was already hot before the arrival of the last tuple,
     *      expirationTimestamp if the key just became hot after the arrival of the last tuple
     */
    private int isHotSketch(int keyId){
        boolean isHot = hotKeys.contains(keyId);
        int freq = Integer.MAX_VALUE;
        total++;
        int result = 1; // 1 means hot, 0 not hot
        if (!isHot){
            freq = countMinSketch.add_and_estimate(keyId,1);
            if (freq > threshold){
                hotKeys.add(keyId);
                isHot = true;
                result = nextUpdateHot + hotInterval;
            }
            else{
                result = 0;
            }
        }

        return result;
    }

    /**
     *
     * @param keyId the key of thenewly arrived tuple
     * @param numOfDistinctKeys number of distinct keys (used to decide whether to use countMin sketch or exact stats
     * @return 0 if the key is not hot, 1 if the key was already hot before the arrival of the last tuple,
     *      expirationTimestamp if the key just became hot after the arrival of the last tuple
     */
    public int isHot(int keyId, int ts, int numOfDistinctKeys){
        if (ts >= nextUpdateHot){
            if (usingSketch){
                countMinSketch.clear();
            }
            else{
                keysStatistics.clear();
            }
            usingSketch = (numOfDistinctKeys >= 10000);
            hotKeys.clear();
            nextUpdateHot += hotInterval;

            threshold = total/numWorkers;

            total = 0;
        }
        return usingSketch ? isHotSketch(keyId) : isHotExact(keyId);
    }

    public int getExpirationTs(){
        return nextUpdateHot + hotInterval;
    }

    public int getTotal(){
        return total;
    }

    public void setFrequencyThreshold(int t){
        threshold = t/numWorkers;
    }

    public void setHotInterval(int h){
        hotInterval = h;
    }
}
