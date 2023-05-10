package KeyGrouping.PStreamForReview;


import KeyGrouping.PStreamForReview.bolt.CoinBolt;
import KeyGrouping.PStreamForReview.bolt.PredictorBolt;
import KeyGrouping.PStreamForReview.bolt.SchedulerBolt;
import org.apache.storm.topology.TopologyBuilder;


public class SchedulingTopologyBuilder extends TopologyBuilder {

    private int schedulingNum=0;

    public void setDifferentiatedScheduling(String UPStreamCompoentID,String UPStreamCompoentIDFields){
        SchedulerBolt schedulerBolt=new SchedulerBolt(UPStreamCompoentID,UPStreamCompoentIDFields);
        CoinBolt coinBolt=new CoinBolt();
        PredictorBolt predictorBolt=new PredictorBolt();
        this.setBolt(Constraints.SCHEDULER_BOLT_ID+schedulingNum, schedulerBolt, 7).shuffleGrouping(UPStreamCompoentID).allGrouping(Constraints.PREDICTOR_BOLT_ID+schedulingNum);
        this.setBolt(Constraints.COIN_BOLT_ID+schedulingNum, coinBolt, 7).shuffleGrouping(Constraints.SCHEDULER_BOLT_ID+schedulingNum, Constraints.coinFileds);
        this.setBolt(Constraints.PREDICTOR_BOLT_ID+schedulingNum, predictorBolt,1).globalGrouping(Constraints.COIN_BOLT_ID+schedulingNum);
        schedulingNum++;
    }

    public int getSchedulingNum() {
        return schedulingNum-1;
    }

}
