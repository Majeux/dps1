package aggregation;

import org.apache.storm.tuple.Tuple;
import java.io.Serializable;

// Container to easily aggregate over just the price (with dangling event_time)
public class AggregationResult implements Serializable {
    public Integer price;
    public Double event_time;

    public AggregationResult(Integer _price, Double _event_time) {
        price = _price;
        event_time = _event_time;
    }

    public AggregationResult(Tuple x) {
        this(x.getIntegerByField("price"), x.getDoubleByField("event_time"));
    }

    public String print() {
        return ", sum price: " + Integer.toString(price) + ", lowest time" + Double.toString(event_time) + "\n";
    }
}
