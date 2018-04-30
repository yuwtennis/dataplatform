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
import java.util.Properties;
import java.time.format.DateTimeFormatter;
import java.time.ZonedDateTime;
import java.io.IOException;

/*
 * This class will tranform messages into elasticsearch readable json
 */

public class EsJsonTransfomer extends BaseBasicBolt {
  /*
   * Default values. These should go into config file since they are also used when building topology
   *  Field names in elastic search index
   */
  private static final String SYS_PROP="system.properties";
  private String esTarget;
  private String kafkaTarget;
  private String esFieldTs;
  private String esFieldMsg;

  public EsJsonTransfomer() {
    // Load all properties needed in this class
    loadProp();   
  }

  @Override
  public void declareOutputFields( OutputFieldsDeclarer declarer ) {
    declarer.declare( new Fields( this.esTarget ) );
  }

  @Override
  public void execute( Tuple tuple,
                       BasicOutputCollector outputCollector) {
    // Retrieve the message from tuple 
    String message = tuple.getStringByField( this.kafkaTarget );

    // Generate ES field and emit the tuple
    outputCollector.emit( new Values( setEsSrc( message ) ) );
  }

  private String setEsSrc( String msg ) {
    // This will return JSON string for source field
    return new JSONObject().put( this.esFieldTs,  genTimestamp() )
                           .put( this.esFieldMsg, msg )
                           .toString();
  }

  private String genTimestamp () {
    // Prepare local date object
    ZonedDateTime datetime = ZonedDateTime.now();

    // Format date object into ISO8601 format
    String text = datetime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

    return text;
  }

  private void loadProp () {
      try {
          // Create Properties class object
          Properties property = new Properties();

          // Load the lines from using inputstream
          property.load( proj.storm.kafka.spout.Topology.class.getClassLoader().getResourceAsStream( SYS_PROP ) );

          this.esTarget    = property.getProperty("es.bolt.target");
          this.kafkaTarget = property.getProperty("storm.bolt.field.kafka");
          this.esFieldTs   = property.getProperty("es.field.timestamp");
          this.esFieldMsg  = property.getProperty("es.field.message");
      } catch ( IOException e ) {
          System.out.println("Property file does not exist. Quitting");
          System.exit(0);
      }
  }

}
