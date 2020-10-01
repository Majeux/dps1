import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.sql.runtime.datasource.socket.spout.SocketSpout; // todo: implement
import org.apache.storm.starter.spout.RandomIntegerSpout; // todo: TEST, remove later
import org.apache.storm.spout.RawScheme;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.operations.CombinerAggregator;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.streams.PairStream.*;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.generated.*;


class AggregateSum {
	public static void main(String[] args) {
		StreamBuilder builder = new StreamBuilder();

		builder.newStream(new RandomIntegerSpout(), new ValueMapper<Integer>(0))
			.window(SlidingWindows.of(Count.of(10), Count.of(2)))
			.mapToPair(w -> Pair.of("test", w))
			.reduceByKey((x, y) -> x + y) //reduce is makkelijker te laten werken, replace met aggregate
			.print();

		Config config = new Config();
		config.setNumWorkers(1);
		try { StormSubmitter.submitTopologyWithProgressBar("agsum", config, builder.build()); }
		catch(AlreadyAliveException e) { System.out.println("Already alive"); }
		catch(InvalidTopologyException e) { System.out.println("Invalid topolgy"); }
		catch(AuthorizationException e) { System.out.println("Auth problem"); }
 	}

	// TODO: laat dit werken met pairstreams (aggregatebykey)
	private static class Sum implements CombinerAggregator<Long, Long, Long> {
	    @Override // The initial value of the sum
	    public Long init() { return 0L; }

	    @Override // Updates the sum by adding the value (this could be a partial sum)
	    public Long apply(Long aggregate, Long value) { return aggregate + value; }

	    @Override // merges the partial sums
	    public Long merge(Long accum1, Long accum2) { return accum1 + accum2; }

	    @Override // extract result from the accumulator (here the accumulator and result is the same)
	    public Long result(Long accum) { return accum; }
	}
}
