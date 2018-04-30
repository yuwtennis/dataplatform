package proj.storm.kafka.spout;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import java.util.UUID;
import java.time.format.DateTimeFormatter;
import java.time.ZonedDateTime;

/*
 * Extracts message from kafka.
 */

public class KafkaExtractor extends BaseBasicBolt {
  /*
   * Default values. These should go into config file since they are also used when building topology
   *  Field names in elastic search index
   */
  private static final String ES_FD_ID   = "id";
  private static final String ES_FD_IDX  = "index";
  private static final String ES_FD_TYPE = "type";
  private static final String ES_FD_SRC  = "source";
  private static final String ES_FD_TS   = "timestamp";
  private static final String ES_FD_MSG  = "message";

  // Ideally elasticesarch index should be 1 to 1 relation with the kafka topic
  private static final String ES_VL_IDX  = "myfirsttopology";

  // Type will be depricated in es 7.x
  private static final String ES_VL_TYPE = "blah";

  private static final String KAFKA_FD   = "str";

  @Override
  public void declareOutputFields( OutputFieldsDeclarer declarer ) {
    declarer.declare( new Fields( ES_FD_ID, ES_FD_IDX, ES_FD_TYPE, ES_FD_SRC ) );
  }

  @Override
  public void execute( Tuple tuple,
                       BasicOutputCollector outputCollector) {
    // Retrieve the message from tuple 
    String message = tuple.getStringByField( KAFKA_FD );

    // Generate ES field and emit the tuple
    outputCollector.emit( new Values( setEsId(), ES_VL_IDX, ES_VL_TYPE, setEsSrc( message ) ) );
  }

  private String setEsId () {
    // This method will generate random uuid.
    // However, making es gernerate its own by POST request is recommended.
    return UUID.randomUUID().toString();
  }

  private String setEsSrc( String msg ) {
    // This will return JSON string for source field
    return new JSONObject().put( ES_FD_TS,  genTimestamp() )
                           .put( ES_FD_MSG, msg )
                           .toString();
  }

  private String genTimestamp () {
    // Prepare local date object
    ZonedDateTime datetime = ZonedDateTime.now();

    // Format date object into ISO8601 format
    String text = datetime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

    return text;
  }
}
