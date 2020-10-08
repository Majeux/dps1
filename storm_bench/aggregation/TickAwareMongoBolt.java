package aggregation;

import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;


// Mongo bolt that ignores tick tuples
public class TickAwareMongoBolt extends MongoInsertBolt {
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
