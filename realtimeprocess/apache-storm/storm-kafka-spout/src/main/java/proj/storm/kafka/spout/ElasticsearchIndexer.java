package proj.storm.kafka.spout;

import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.elasticsearch.common.EsConfig;
import org.apache.storm.elasticsearch.common.DefaultEsTupleMapper;
import org.apache.storm.elasticsearch.common.EsTupleMapper;
import org.apache.storm.elasticsearch.bolt.EsIndexBolt;
import java.util.HashMap;
import java.util.Map;

public class ElasticsearchIndexer extends BaseBasicBolt {
  private static final String ES_HOST    = "http://localhost:9200";

  @Override
  public void declareOutputFields( OutputFieldsDeclarer declarer ) {
    // Nothing to emit. No need for naming FIELD
  }

  @Override
  public void execute( Tuple tuple,
                       BasicOutputCollector collector ) {
    // Prepare elasticsearch connection parameter
    EsConfig EsConfig = new EsConfig( new String[]{ ES_HOST } );

    // Transoport client compatitable to elasticsearch 5.5.3
    // https://mvnrepository.com/artifact/org.apache.storm/storm-elasticsearch/1.2.1
    EsTupleMapper tupleMapper = new DefaultEsTupleMapper();

    // Fire Index request to elasticsearch data node
    EsIndexBolt indexBolt = new EsIndexBolt(EsConfig , tupleMapper);
  }
}
