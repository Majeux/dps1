package aggregation;
import aggregation.AggregationResult;

// Storm
import org.apache.storm.cassandra.trident.state.SimpleTuple;
import org.apache.storm.streams.operations.Function;
import org.apache.storm.streams.Pair;
import org.apache.storm.tuple.Fields;

// Utils
import java.util.Arrays;

// Map aggregation tuples to a nice output format:
// Aggregate result: {gemID, AggregationResult = {price, event_time}}
// =>
// Mongo entry: {GemID, aggregate, latency}
public class ToOutputTuple implements Function<Pair<Integer,AggregationResult>, SimpleTuple> {
    
    public ToOutputTuple() {}

    @Override
    public SimpleTuple apply(Pair<Integer, AggregationResult> input) {
        Fields outputFields = new Fields(Arrays.asList("GemID", "aggregate", "max_event_time"));

        int gemID = input.getFirst();
        String aggregate = Integer.toString(input.getSecond().price);
        Double highest_event_time = input.getSecond().event_time;

        SimpleTuple tuple = new SimpleTuple(outputFields, Arrays.asList(gemID, aggregate, highest_event_time));
        return tuple;
    }
}
