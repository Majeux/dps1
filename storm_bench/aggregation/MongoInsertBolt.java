package aggregation;

import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Arrays;
import org.apache.storm.tuple.Fields;
import org.apache.commons.net.ntp.TimeStamp;
import java.net.InetAddress;
import java.time.Instant;
import org.apache.commons.net.ntp.NTPUDPClient;
import java.net.SocketException;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.apache.storm.mongodb.bolt.AbstractMongoBolt;
import org.apache.storm.cassandra.trident.state.SimpleTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.TupleUtils;

public class MongoInsertBolt extends AbstractMongoBolt {

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;
    private MongoMapper mapper;
    private boolean ordered = true;  //default is ordered.
    private TimeGetter timeGetter = new SystemTime();
    
    public MongoInsertBolt(String url, String collectionName, MongoMapper mapper) {
        super(url, collectionName);
        Validate.notNull(mapper, "MongoMapper can not be null");
        this.mapper = mapper;
    }

    @Override
    public void execute(Tuple tuple) {
        if(TupleUtils.isTick(tuple)) { return; }
        SimpleTuple tup = (SimpleTuple)tuple.getValue(0);

        // Calculate latency at the moment before output
        Double max_event_time = tup.getDoubleByField("max_event_time");
        Double cur_time = timeGetter.get();
        Double latency = cur_time - max_event_time;

        // Build up the final result tuple
        Fields outputFields = new Fields(Arrays.asList("GemID", "aggregate", "latency", "time"));
        List<Object> outputValues = new ArrayList(tup.getValues());
        outputValues.set(2, latency);
        outputValues.add(cur_time);

        SimpleTuple outputTuple = new SimpleTuple(outputFields, outputValues);
        mongoClient.insert(Arrays.asList(mapper.toDocument(outputTuple)), ordered);
    }
 

    public MongoInsertBolt withOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(topoConf, context, collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    private interface TimeGetter {
        public Double get();
    }

    // Gets time from NTP server
    private class NTPTime implements TimeGetter, Serializable {
	String NTP_IP;

        public NTPTime(String _NTP_IP) {
            NTP_IP = _NTP_IP;
        }

        @Override
        public Double get() {
            final NTPUDPClient client = new NTPUDPClient();
            try { client.open(); }
            catch (final SocketException e) { System.out.println("Could not establish NTP connection"); }

            Double time = 0.0;
            try {
                TimeStamp recv_time = client
                    .getTime(InetAddress.getByName(NTP_IP))
                    .getMessage()
                    .getReceiveTimeStamp();

                Double integer_part = Long.valueOf(recv_time.getSeconds()).doubleValue();
                Double fraction = Long.valueOf(recv_time.getFraction()).doubleValue() / 0xFFFFFFFF;

                return integer_part + fraction;
            }
            catch (IOException e) { System.out.println("Could not get time from NTP server"); }
            // NTP request has failed 
            return 0.0;
        }
    }

    // Gets time from system clock
    private class SystemTime implements TimeGetter, Serializable {
        @Override
        public Double get() {
            Instant time = Instant.now();
            return Double.valueOf(time.getEpochSecond()) + Double.valueOf(time.getNano()) / (1000.0*1000*1000);
        }
    }
}
