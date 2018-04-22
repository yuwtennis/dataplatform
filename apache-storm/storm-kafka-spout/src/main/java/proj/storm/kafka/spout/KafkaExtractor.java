package proj.storm.kafka.spout;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.tuple.Tuple;

import java.util.List;

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
    String message = tuple.getStringByField( "str" );

    System.out.format( "### Processed message ### %s\n" , message );
  }

  private void dumpTuple( Tuple tuple ) {
    //  Extracts event from tuple.
    List tupleFields = tuple.getFields( ).toList();
    List tupleValues = tuple.getValues( );

    for( int i = 0 ; i < tupleFields.size() ; i++ ) {
      System.out.format( "### Fields inside tuple ### idx : %d , val : %s\n", i, tupleFields.get(i).toString() );
    }

    for( int i = 0 ; i < tupleValues.size() ; i++ ) {
      System.out.format( "### Values inside tuple ### idx : %d , val : %s\n", i, tupleValues.get(i).toString() );
    }
  }

}
