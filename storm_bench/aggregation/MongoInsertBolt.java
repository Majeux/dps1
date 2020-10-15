package aggregation;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.BatchHelper;
import org.apache.storm.utils.TupleUtils;
import org.bson.Document;
import org.apache.storm.mongodb.bolt.AbstractMongoBolt;
import org.apache.storm.cassandra.trident.state.SimpleTuple;

public class MongoInsertBolt extends AbstractMongoBolt {

    private static final int DEFAULT_FLUSH_INTERVAL_SECS = 1;
    private MongoMapper mapper;
    private boolean ordered = true;  //default is ordered.
    private int batchSize;
    private BatchHelper batchHelper;
    private int flushIntervalSecs = DEFAULT_FLUSH_INTERVAL_SECS;
    public MongoInsertBolt(String url, String collectionName, MongoMapper mapper) {
        super(url, collectionName);
        Validate.notNull(mapper, "MongoMapper can not be null");
        this.mapper = mapper;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (batchHelper.shouldHandle(tuple)) {
                batchHelper.addBatch(tuple);
            }

            if (batchHelper.shouldFlush()) {
                flushTuples();
                batchHelper.ack();
            }
        } catch (Exception e) {
            batchHelper.fail(e);
        }
    }

    private void flushTuples() {
        List<Document> docs = new LinkedList<>();
        for (Tuple t : batchHelper.getBatchTuples()) {
            Document doc = mapper.toDocument((SimpleTuple)t.getValue(0));
            docs.add(doc);
        }
        mongoClient.insert(docs, ordered);
    }

    public MongoInsertBolt withBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public MongoInsertBolt withOrdered(boolean ordered) {
        this.ordered = ordered;
        return this;
    }

    public MongoInsertBolt withFlushIntervalSecs(int flushIntervalSecs) {
        this.flushIntervalSecs = flushIntervalSecs;
        return this;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return TupleUtils.putTickFrequencyIntoComponentConfig(super.getComponentConfiguration(), flushIntervalSecs);
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(topoConf, context, collector);
        this.batchHelper = new BatchHelper(batchSize, collector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
