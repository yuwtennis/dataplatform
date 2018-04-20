package proj.storm.kafka.spout;

// List import packages here
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;

import java.util.UUID;

import proj.storm.kafka.spout.KafkaExtractor;

// Define topology
public class Topology {
  // Instance Value here

  // Method here
  public static void main( String[] args ) {
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
    /* Set up storm configuration
    /* ****************************************************************** */
    /*
       Based on common config parameters
       http://storm.apache.org/releases/1.2.1/Running-topologies-on-a-production-cluster.html
    */

    Config conf = new Config();

    // Config.TOPOLOGY_WORKERS
    conf.setNumWorkers(1);

    // Config.TOPOLOGY_ACKER_EXECUTORS
    conf.setNumAckers(1);

    // Config.TOPOLOGY_MAX_SPOUT_PENDING
    conf.setMaxSpoutPending(5000);

    // Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
    // Default
    conf.setMessageTimeoutSecs(30);

    // conf.registerSerialization();

    StormSubmitter.submitTopology("mytopology", conf, topology);

    /* ****************************************************************** */
    /* Set up topology                                                    */
    /* ****************************************************************** */
    /* Add Spout                                                          */
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout( "kafka-spout" , kafkaSpout );

    /* Add Bolt                                                           */
    builder
      .setBolt("kafka-extractor", new KafkaExtractor())
      .shuffleGrouping( "kafka-spout" );

    /* Finalize the topology                                              */
    StormTopology topology = builder.createTopology();

    /* Submit to storm                                                    */
    StormSubmitter.submitTopology("mytopology", conf, topology);
  }
}

