package proj.storm.kafka.spout;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.tuple.Tuple;

/*
 * Simple Bolt outputs result to standard output
 */

public class KafkaExtractor extends BaseBasicBolt {
  @Override
  public void declareOutputFields( OutputFieldsDeclarer declarer ) {
    /* This is the last bolt so write nothing */
  }

  @Override
  public void execute( Tuple tuple,
                       BasicOutputCollector outputCollector) {
    /*  Extracts event from tuple. */
    String message = tuple.getStringByField("kafka-spout");

    /* Just log to standard output */
    System.out.format("Kafka Message : %s", message);
  }
}
