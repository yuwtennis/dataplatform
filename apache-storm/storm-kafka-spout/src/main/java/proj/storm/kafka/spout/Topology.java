package proj.storm.kafka.spout;

// List import packages here
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

// Define topology
public class Topology {
  // Instance Value here

  // Method here
  public static void main( String[] args ) throws Exception {
    /* ********************************************************************** */
    /* Kafka configuration variable (This needs to be in configuration file!) */
    /* ********************************************************************** */
    // Kafka consumer client depends on Zookeeper when finding kafka nodes.
    // Zookeeper Host List
    String zkConnString = "localhost:2181";
    String clusterName  = "cluster";
    String topicName    = "myfirsttopic";

    /* ****************************************************************** */
    /* Build kafka consumer spout                                         */
    /* ****************************************************************** */
    // Build zookeeper instance
    BrokerHosts hosts = new ZkHosts(zkConnString);

    // Build configuration instance for Spout
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());

    // Build Multischeme instance
    spoutConfig.scheme = new SchemeAsMultiScheme( new StringScheme() );

    // Build Kafka spout
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

    /* ****************************************************************** */
    /* Set up the whole topology                                          */
    /* ****************************************************************** */
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout( "kafka-spout" , kafkaSpout );
  }
}

