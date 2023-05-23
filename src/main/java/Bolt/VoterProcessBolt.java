package Bolt;

import Util.Conf;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

public class VoterProcessBolt extends BaseRichBolt {
    private HashMap<String, Long> counts;
    private HashMap<String, List<String>> result;
    private OutputCollector collector;
    private boolean enableLog;
    private FileHandler handler;
    private Logger LOGGER;
    NTPUDPClient timeClient;
    private double totalProcessTime;
    private long totalProcessTuple;
    private Timer timer;
    private int boltID;
    private int ticks;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.boltID = topologyContext.getThisTaskId();
        this.collector = outputCollector;
        this.totalProcessTime = 0l;
        this.totalProcessTuple = 0l;
        this.enableLog = true;
        this.timer = new Timer();
        this.timeClient = new NTPUDPClient();
        this.ticks = 0;
        counts = new HashMap<>();
        result = new HashMap<>();

        if(this.enableLog) {
            this.LOGGER = Logger.getLogger(String.valueOf(boltID));
            String logFileName = "/log/voteCounterBolt-" + String.valueOf(boltID) + ".log";
            try {
                this.handler = new FileHandler(logFileName, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            handler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    String message = record.getMessage();
                    if (message.endsWith(System.lineSeparator())) {
                        message = message.substring(0, message.length() - System.lineSeparator().length());
                    }
                    return "[" + record.getLevel() + "] " + message + System.lineSeparator();
                }
            });
            LOGGER.addHandler(handler);
        }

        timer.schedule(new update(), 60 * 1000, 60 * 1000);
    }

    @Override
    public void execute(Tuple tuple) {
        String zip_code = tuple.getStringByField("zip_code");
        String county_id = tuple.getStringByField("county_id");
        String last_name = tuple.getStringByField("last_name");
        String first_name = tuple.getStringByField("first_name");
        String middle_name = tuple.getStringByField("middle_name");
        String full_phone_number = tuple.getStringByField("full_phone_number");
        String party_cd = tuple.getStringByField("party_cd");
        String gender_code = tuple.getStringByField("gender_code");
        String birth_year = tuple.getStringByField("birth_year");
        String age_at_year_end = tuple.getStringByField("age_at_year_end");
        String birth_state = tuple.getStringByField("birth_state");
        String rescue_dist_abbrv = tuple.getStringByField("rescue_dist_abbrv");
        String rescue_dist_desc = tuple.getStringByField("rescue_dist_desc");
        String munic_dist_abbrv = tuple.getStringByField("munic_dist_abbrv");
        String munic_dist_desc = tuple.getStringByField("munic_dist_desc");
        String dist_1_abbrv = tuple.getStringByField("dist_1_abbrv");
        String dist_1_desc = tuple.getStringByField("dist_1_desc");
        String vtd_abbrv = tuple.getStringByField("vtd_abbrv");
        String vtd_desc = tuple.getStringByField("vtd_desc");

        Long count = counts.get(zip_code);
        if (count == null) {
            count = 0L;
        }
        count++;
        counts.put(zip_code, count);

        StringBuilder sb = new StringBuilder();
        sb.append(county_id).append(" ");
        sb.append(last_name).append(" ");
        sb.append(first_name).append(" ");
        sb.append(middle_name).append(" ");
        sb.append(full_phone_number).append(" ");
        sb.append(party_cd).append(" ");
        sb.append(gender_code).append(" ");
        sb.append(birth_year).append(" ");
        sb.append(age_at_year_end).append(" ");
        sb.append(birth_state).append(" ");
        sb.append(rescue_dist_abbrv).append(" ");
        sb.append(rescue_dist_desc).append(" ");
        sb.append(munic_dist_abbrv).append(" ");
        sb.append(munic_dist_desc).append(" ");
        sb.append(dist_1_abbrv).append(" ");
        sb.append(dist_1_desc).append(" ");
        sb.append(vtd_abbrv).append(" ");
        sb.append(vtd_desc);

        String str = sb.toString();


        if(!result.containsKey(zip_code)) {
            List<String> list = new ArrayList<>();
            result.put(zip_code, list);
        }
        result.get(zip_code).add(str);
        totalProcessTuple++;
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private class update extends TimerTask {

        @Override
        public void run() {
            ticks++;
            if(enableLog) {
                LOGGER.log(Level.INFO,ticks + " --- ReviewProcess Bolt " + boltID + " process Tuples: " + totalProcessTuple + " averageProcessTime: " + totalProcessTime / totalProcessTuple + " ms");
            } else {
                System.out.println(ticks + " --- ReviewProcess Bolt " + boltID + " process Tuples: " + totalProcessTuple + " averageProcessTime: " + totalProcessTime / totalProcessTuple + " ms");
            }
            totalProcessTime = 0;
            totalProcessTuple = 0;
        }
    }

    private long getCurTime() throws IOException {
        InetAddress inetAddress = InetAddress.getByName(Conf.NTP_SERVER);
        TimeInfo timeInfo = timeClient.getTime(inetAddress);
        long currentTimeMillis = timeInfo.getMessage().getTransmitTimeStamp().getTime();
        return currentTimeMillis;
    }
}
