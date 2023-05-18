
package KeyGrouping.Dalton.prompt;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Stats implements Serializable {
    Map<Integer, Integer> counts = new HashMap<Integer, Integer>();

    public void incCount(int key){
        counts.put(key, counts.getOrDefault(key, 0) + 1);
    }

    public int getKey(int key){
        return counts.getOrDefault(key, 0);
    }

    public int size(){
        return counts.size();
    }

    public void clear(){
        counts.clear();
    }
}
