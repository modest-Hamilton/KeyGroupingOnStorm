

package KeyGrouping.Dalton.containers;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class PartialAssignment implements Serializable {
    public Map<Integer, BitSet> keyAssignment;
    public long timestamp;

    public PartialAssignment(){
        keyAssignment = new HashMap<>();
    }

    public void clear(){
        keyAssignment.clear();
        timestamp = 0;
    }
}
