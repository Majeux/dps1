package aggregation;
import aggregation.ToOutputTuple;
import aggregation.SumAggregator;
import aggregation.MongoInsertBolt;
import aggregation.StupidSpout;

// STORM
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.sql.runtime.datasource.socket.spout.SocketSpout;
import org.apache.storm.sql.runtime.serde.json.JsonScheme;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.windowing.SlidingWindows;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.streams.Pair;
import org.apache.storm.generated.*;
import org.apache.storm.spout.Scheme; // REMOVE along WITH STUPIDSPOUT
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
//import org.apache.storm.mongodb.bolt.MongoInsertBolt;

// AUXILLIARY
import java.util.Arrays;


// Main class to submit to the storm cluser. Contains a stream API construction of a topology
public class AggregateSum {
    public static void main(String[] args) {
        // Get arguments
        if(args.length < 4) { System.out.println("Must supply input_ip, input_port, mongo_ip, and num_workers"); }
        String input_IP = args[0];
        String input_port = args[1];
        String mongo_IP = args[2];
        Integer num_workers = Integer.parseInt(args[3]);
        String NTP_IP = "";
        if(args.length > 4) { NTP_IP = args[4]; }

        // Socket spout to get input tuples
        JsonScheme inputScheme = new JsonScheme(Arrays.asList("gem", "price", "event_time"));
        //SocketSpout sSpout = new SocketSpout(inputScheme, input_IP, Integer.parseInt(input_port));
        StupidSpout sSpout = new StupidSpout(inputScheme, input_IP, Integer.parseInt(input_port));

        // Mongo bolt to store the results
        String mongo_addr = "mongodb://storm:test@" + mongo_IP + ":27017/results?authSource=admin";
        SimpleMongoMapper mongoMapper = new SimpleMongoMapper().withFields("GemID", "aggregate", "latency");
	MongoInsertBolt mongoBolt = new MongoInsertBolt(mongo_addr, "aggregation", mongoMapper);

        // Build a stream
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(sSpout)
            .window(SlidingWindows.of(Count.of(8), Count.of(4)))
            .mapToPair(x -> Pair.of(x.getIntegerByField("gem"), new Values(x)))
            .aggregateByKey(new SumAggregator())
            .map(new ToOutputTuple(NTP_IP))
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
}
