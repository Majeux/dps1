package aggregation;

// STORM
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.sql.runtime.datasource.socket.spout.SocketSpout; // todo: implement
import org.apache.storm.starter.spout.RandomIntegerSpout; // todo: TEST, remove later
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
import org.apache.storm.streams.operations.Consumer;

// AUXILLIARY
import java.util.List;
import java.util.Arrays;
import java.lang.Math;
import java.io.FileWriter;
import java.io.IOException;


public class AggregateSum {
    static List<String> fields = Arrays.asList("gem", "price", "event_time");
    
    public static void main(String[] args) {
        String IP = args[0];
        String PORT = args[1];
        Integer num_workers = Integer.parseInt(args[2]);
    
        StreamBuilder builder = new StreamBuilder();

        builder.newStream(new SocketSpout(new JsonScheme(fields), IP, Integer.parseInt(PORT)))
	    	.window(SlidingWindows.of(Duration.seconds(8), Duration.seconds(4)))
            .mapToPair(x -> Pair.of(x.getIntegerByField("gem"), new Values(x)))
	        .aggregateByKey(new Sum())
            .forEach(new Print());

        // Build config and submit
	    Config config = new Config();
	    config.setNumWorkers(num_workers);
        config.setDebug(true);
	   	try { StormSubmitter.submitTopologyWithProgressBar("agsum", config, builder.build()); }
	   	catch(AlreadyAliveException e) { System.out.println("Already alive"); }
	   	catch(InvalidTopologyException e) { System.out.println("Invalid topolgy"); }
	   	catch(AuthorizationException e) { System.out.println("Auth problem"); }
 	}

    private static class Print implements Consumer<Pair<Integer, Values>> {
        @Override 
        public void accept(Pair<Integer, Values> input) {
            System.out.println("GemID: " + input.getFirst() + input.getSecond().print());
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

