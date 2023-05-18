
package KeyGrouping.Dalton.containers;

import java.io.Serializable;

public class PartialLoad implements Serializable{
    public int load;
    public long timestamp;

    public PartialLoad(){
        load = 0;
    }

    public void clear(){
        timestamp = 0;
        load = 0;
    }
}
