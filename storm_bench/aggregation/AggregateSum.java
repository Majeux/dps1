package aggregation;
import aggregation.ToOutputTuple;
import aggregation.SumAggregator;
import aggregation.TickAwareMongoBolt;

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
    static List<String> fields = Arrays.asList("gem", "price", "event_time");

    public static void main(String[] args) {
        // Get arguments
        if(args.length < 4) { System.out.println("Must supply input_ip, input_port, mongo_ip, and num_workers"); }
        String input_IP = args[0];
        String input_PORT = args[1];
        String mongo_IP = args[2];
        Integer num_workers = Integer.parseInt(args[3]);
        String NTP_IP = "";
        if(args.length >= 4) { NTP_IP = args[4]; }

        // Mongo bolt to store the results
        String mongo_addr = "mongodb://storm:test@" + mongo_IP + ":27017/&authSource=results";
        SimpleMongoMapper mongoMapper = new SimpleMongoMapper().withFields("GemID", "aggregate", "latency");
        TickAwareMongoBolt mongoBolt = new TickAwareMongoBolt(mongo_addr, "aggregation", mongoMapper);

        // Build a stream
        StreamBuilder builder = new StreamBuilder();
        builder.newStream(new SocketSpout(new JsonScheme(fields), input_IP, Integer.parseInt(input_PORT)))
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
