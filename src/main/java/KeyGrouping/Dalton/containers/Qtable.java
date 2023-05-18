
package KeyGrouping.Dalton.containers;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing the Qtable for our contextual bandits implementation
 */
public class Qtable implements Serializable {
    private Map<Integer, QtableEntry> qtable; // key -> qvalues

    public Qtable(){
        qtable = null;
    }

    public Qtable(int numOfKeys){
        qtable = new HashMap<>(numOfKeys);
    }

    public double[] get(int key){
        return qtable.get(key).qvalues;
    }

    public void put(int key, double[] values, int ts){
        qtable.put(key, new QtableEntry(values, ts));
    }

    public boolean containsKey(int key){
        return qtable.containsKey(key);
    }

    public int getExpirationTs(int key){
        return qtable.get(key).expirationTs;
    }

    public void remove(int key){
        qtable.remove(key);
    }

    public void setExpTs(int key, int ts){
        qtable.get(key).expirationTs = ts;
    }

    public boolean isEmpty(){
        return qtable.isEmpty();
    }

    public Map<Integer, QtableEntry> getTable(){
        return qtable;
    }

    public void clear(){
        qtable.clear();
    }

    public int size(){
        return qtable.size();
    }
}
