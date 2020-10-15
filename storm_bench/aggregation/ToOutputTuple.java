package aggregation;
import aggregation.AggregationResult;

// Storm
import org.apache.storm.cassandra.trident.state.SimpleTuple;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.Pair;

// Utils
import org.apache.commons.net.ntp.NTPUDPClient;
import java.net.SocketException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.storm.tuple.Fields;
import org.apache.commons.net.ntp.TimeStamp;
import java.net.InetAddress;
import java.time.Instant;
import java.io.Serializable;


// Map aggregation tuples to a nice output format:
// Aggregate result: {gemID, AggregationResult = {price, event_time}}
// =>
// Mongo entry: {GemID, aggregate, latency}
public class ToOutputTuple implements Function<Pair<Integer,AggregationResult>, SimpleTuple> {
    private String NTP_IP = "";
    private TimeGetter timeGetter;

    private interface TimeGetter {
        public Double get();
    }

    // Gets time from NTP server
    private class NTPTime implements TimeGetter, Serializable {
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
    private class systemTime implements TimeGetter, Serializable {
        @Override
        public Double get() {
            Instant time = Instant.now();
            return Double.valueOf(time.getEpochSecond()) + Double.valueOf(time.getNano()) / (1000.0*1000*1000);
        }
    }
    
    public ToOutputTuple(String _NTP_IP) {
        NTP_IP = _NTP_IP;
        if(NTP_IP == "") { timeGetter = new systemTime(); }
        else { timeGetter = new NTPTime(); }
    }

    @Override
    public SimpleTuple apply(Pair<Integer, AggregationResult> input) {
        Fields outputFields = new Fields(Arrays.asList("GemID", "aggregate", "latency"));

        int gemID = input.getFirst();
        String aggregate = Integer.toString(input.getSecond().price);
        Double lowest_event_time = input.getSecond().event_time;
        String latency = Double.toString(timeGetter.get() - lowest_event_time);

        SimpleTuple tuple = new SimpleTuple(outputFields, Arrays.asList(gemID, aggregate, latency));
        return tuple;
    }
}
