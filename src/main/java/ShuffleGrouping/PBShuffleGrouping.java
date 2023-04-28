package ShuffleGrouping;


import KeyGrouping.CKGrouping;
import Util.Conf;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class PBShuffleGrouping implements CustomStreamGrouping {
    private List<Integer> targetTasks;
    private int numServers;
    private HashMap<Integer, Long> boltWeight;
    private Timer timer;
    private Jedis jedis;
    @Override
    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
        this.numServers = list.size();
        this.boltWeight = new HashMap<>();
        this.jedis = new Jedis(Conf.REDIS_HOST, Conf.REDIS_PORT);
        this.timer = new Timer();
        timer.schedule(new updateWeight(), 5000,5000);
    }

    @Override
    public List<Integer> chooseTasks(int i, List<Object> list) {
        return null;
    }

    private class updateWeight extends TimerTask {

        @Override
        public void run() {
            for(int i = 0;i < numServers;i++) {
                boltWeight.put(i, Long.valueOf(jedis.get(String.valueOf(i))));
            }
        }
    }
}
