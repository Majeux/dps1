package aggregation;

import org.apache.storm.tuple.Tuple;

// Container to easily aggregate over just the price (with dangling event_time)
public class Values {
    public Integer price;
    public Double event_time;

    public Values(Integer _price, Double _event_time) {
        price = _price;
        event_time = _event_time;
    }

    public Values(Tuple x) {
        this(x.getIntegerByField("price"), x.getDoubleByField("event_time"));
    }

    public String print() {
        return ", sum price: " + Integer.toString(price) + ", lowest time" + Double.toString(event_time) + "\n";
    }
}
