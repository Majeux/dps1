package aggregation;

// STORM
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.sql.runtime.datasource.socket.spout.SocketSpout; // todo: implement
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

// AUXILLIARY
import java.util.List;
import java.util.Arrays;
import java.lang.Math;
import java.io.FileWriter;
import java.io.IOException;

public class AggregateSum {
    static List<String> fields = Arrays.asList("gem", "price", "event_time");

    public static void main(String[] args) {
        // Get arguments
        String input_IP = args[0];
        String input_PORT = args[1];
        String mongo_IP = args[2];
        String mongo_addr = "mongodb://storm:test@" + mongo_IP + ":27017/&authSource=results";
        Integer num_workers = Integer.parseInt(args[3]);

        // Mongo bolt to store the results
        SimpleMongoMapper mongoMapper = new SimpleMongoMapper().withFields("GemID", "aggregate", "latency");
        MongoInsertBolt mongoBolt = new MongoInsertBolt(mongo_addr, "aggregation", mongoMapper);

        // Build a stream
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(new SocketSpout(new JsonScheme(fields), input_IP, Integer.parseInt(input_PORT)))
            .window(SlidingWindows.of(Duration.seconds(8), Duration.seconds(4)))
            .mapToPair(x -> Pair.of(x.getIntegerByField("gem"), new Values(x)))
	        .aggregateByKey(new Sum())
            .map(new toOutputTuple())
            .to(mongoBolt);

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

    private static class toOutputTuple implements Function<Pair<Integer,Values>, SimpleTuple> {
        Fields outputFields = new Fields(Arrays.asList("GemID", "aggregate", "latency"));

        @Override
        public SimpleTuple apply(Pair<Integer, Values> input) {
            return new SimpleTuple(outputFields,
                Arrays.asList(
                    input.getFirst(),
                    Integer.toString(input.getSecond().price),
                    Double.toString(input.getSecond().event_time)
                )
            );
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
