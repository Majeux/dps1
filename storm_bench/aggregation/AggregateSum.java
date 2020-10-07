package aggregation;

// STORM
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.sql.runtime.datasource.socket.spout.SocketSpout;
import org.apache.storm.sql.runtime.serde.json.JsonScheme;
import org.apache.storm.spout.RawScheme;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.operations.CombinerAggregator;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.streams.PairStream.*;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.generated.*;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.cassandra.trident.state.SimpleTuple;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.TupleUtils;
import org.apache.storm.mongodb.common.mapper.MongoMapper;

// AUXILLIARY
import java.util.List;
import java.util.Arrays;
import java.lang.Math;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.commons.net.ntp.NTPUDPClient;
import java.net.SocketException;
import org.apache.commons.net.ntp.TimeInfo;
import org.apache.commons.net.ntp.TimeStamp;
import java.net.InetAddress;
import java.time.Instant;


public class AggregateSum {

    public static class TickAwareMongoBolt extends MongoInsertBolt {
        public TickAwareMongoBolt(String url, String collectionName, MongoMapper mapper) {
            super(url, collectionName, mapper);
        }
        @Override
        public void execute(Tuple tuple) {
            System.out.println("FIELDS: " + tuple.getFields().toString());

            if(tuple.contains("value")) {
                System.out.println(tuple.getValueByField("value"));
            }

            if(!TupleUtils.isTick(tuple)) super.execute(tuple);
            else {System.out.println("LEL wat moet je met al die ticks");}
        }
    }


    static List<String> fields = Arrays.asList("gem", "price", "event_time");

    public static void main(String[] args) {
        // Get arguments
        String input_IP = args[0];
        String input_PORT = args[1];
        String mongo_IP = args[2];
        String NTP_IP = args[3];
        Integer num_workers = Integer.parseInt(args[4]);
        
        // Mongo bolt to store the results
        String mongo_addr = "mongodb://storm:test@" + mongo_IP + ":27017/&authSource=results";
        SimpleMongoMapper mongoMapper = new SimpleMongoMapper().withFields("GemID", "aggregate", "latency");
        TickAwareMongoBolt mongoBolt = new TickAwareMongoBolt(mongo_addr, "aggregation", mongoMapper);

        // Build a stream
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(new SocketSpout(new JsonScheme(fields), input_IP, Integer.parseInt(input_PORT)))
            //.window(SlidingWindows.of(Duration.seconds(8), Duration.seconds(4)))
            .window(SlidingWindows.of(Count.of(8), Count.of(4)))
            .mapToPair(x -> Pair.of(x.getIntegerByField("gem"), new Values(x)))
	        .aggregateByKey(new Sum())
            //.peek(s -> System.out.println("GemID: " + Integer.toString(s.getFirst()) + ", "+ s.getSecond().print() ))
            .map(new toOutputTuple(NTP_IP))
            .peek(s -> System.out.println(s.getFields().toString()))
            .to(mongoBolt);

        // Build config and submit
	    Config config = new Config();
        config.setMaxSpoutPending(15);
	    config.setNumWorkers(num_workers);
        //config.setDebug(true);
	   	try { StormSubmitter.submitTopologyWithProgressBar("agsum", config, builder.build()); }
	   	catch(AlreadyAliveException e) { System.out.println("Already alive"); }
	   	catch(InvalidTopologyException e) { System.out.println("Invalid topolgy"); }
	   	catch(AuthorizationException e) { System.out.println("Auth problem"); }
 	}

    // Map aggregation tuples to a nice output format:
    // Aggregate result: {gemID, Values = {price, event_time}}
    // =>
    // Mongo entry: {GemID, aggregate, latency}
    private static class toOutputTuple implements Function<Pair<Integer,Values>, SimpleTuple> {
        String NTP_IP = "";

        public toOutputTuple(String _NTP_IP) {
            NTP_IP = _NTP_IP;
        }

        // Gets time from NTP server
        private Double currentNTPTime() {
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
            catch (IOException ioe) { System.out.println("Could not get time from NTP server"); }
            // NTP request has failed 
            return 0.0;
        }

        // Gets time from system clock
        private Double currentTime() {
            Instant time = Instant.now();
            return Double.valueOf(time.getEpochSecond()) + Double.valueOf(time.getNano()) / (1000.0*1000*1000);
        }

        @Override
        public SimpleTuple apply(Pair<Integer, Values> input) {
            System.out.println("apply?");

            Fields outputFields = new Fields(Arrays.asList("GemID", "aggregate", "latency"));
            
            int gemID = input.getFirst();
            String aggregate = Integer.toString(input.getSecond().price);
            Double lowest_event_time = input.getSecond().event_time;
            String latency = Double.toString(currentTime() - lowest_event_time);
            
            SimpleTuple tuple = new SimpleTuple(outputFields, Arrays.asList(gemID, aggregate, latency));
            System.out.println("MADE NEW OUTPUT TUPLE");            
            return tuple;
        }
    }

    // Container to easily aggregate over just the price (with dangling event_time)
    private static class Values {
        Integer price;
        Double event_time;

        public Values(Integer _price, Double _event_time) {
            this.price = _price;
            this.event_time = _event_time;
        }

        public Values(Tuple x) {
            this(x.getIntegerByField("price"), x.getDoubleByField("event_time"));
        }

        public String print() {
            return ", sum price: " + Integer.toString(price) + ", lowest time" + Double.toString(event_time) + "\n";
        }
    }


	private static class Sum implements CombinerAggregator<Values, Values, Values> {
	    @Override // The initial value of the sum
	    public Values init() { return new Values(0, Double.POSITIVE_INFINITY); }

	    @Override // Updates the sum by adding the value (this could be a partial sum)
	    public Values apply(Values aggregate, Values value) {
            return new Values(
                aggregate.price + value.price,
                Math.min(aggregate.event_time, value.event_time)
            );
        }

	    @Override // merges the partial sums
	    public Values merge(Values accum1, Values accum2) {
            return new Values(
                accum1.price + accum2.price,
                Math.min(accum1.event_time, accum2.event_time)
            );
        }

	    @Override // extract result from the accumulator (here the accumulator and result is the same)
	    public Values result(Values accum) { return accum; }
	}
}
