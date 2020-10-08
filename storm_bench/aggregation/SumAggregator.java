package aggregation;
import aggregation.Values;

// Storm
import org.apache.storm.streams.operations.CombinerAggregator;

// Aggregates sum, while finding the minimum event time.
public class SumAggregator implements CombinerAggregator<Values, Values, Values> {
    
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

    @Override // extract result from the accumulator
    public Values result(Values accum) { return accum; }
}
