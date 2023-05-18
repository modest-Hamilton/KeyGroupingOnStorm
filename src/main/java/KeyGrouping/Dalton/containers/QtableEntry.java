
package KeyGrouping.Dalton.containers;

/**
 * Class representing an entry for a specific key inside a Qtable
 */
public class QtableEntry {
    public double[] qvalues;
    public int expirationTs;

    public QtableEntry(double[] vals, int ts){
        qvalues = vals;
        expirationTs = ts;
    }
}
