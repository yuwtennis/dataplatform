package proj.storm.kafka.spout;

// List import packages here
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.utils.Utils;
import org.apache.storm.tuple.Fields;
import java.lang.InterruptedException;
import org.apache.storm.LocalCluster;

import java.util.UUID;

import proj.storm.kafka.spout.KafkaExtractor;

// Define topology
public class Topology {
  // Instance Value here
    /* ********************************************************************** */
    /* Kafka configuration variable (This needs to be in configuration file!) */
    /* ********************************************************************** */
    // Kafka consumer client depends on Zookeeper when finding kafka nodes.
    // Zookeeper Host List
    private static String zkConnString       = "localhost:2181";
    private static String brokerZkPath       = "/kafka-cluster-1/brokers";
    private static String zkRoot             = "/kafka-cluster-1/brokers/topics";
    private static String topicName          = "myfirsttopic";

    /* ****************************************************************** */
    /* Topology configuration variable                                    */
    /* ****************************************************************** */
    // The number of tasks that should be assigned to execute this bolt
    private static int boltParalismHint      = 1;
    private static int spoutParalismHint     = 1;
    private static String  topologyName      = "mytopology";

  // Method here
  public static void main( String[] args ) {

    /* ****************************************************************** */
    /* Build kafka consumer spout                                         */
    /* ****************************************************************** */
    // Build zookeeper instance
    BrokerHosts hosts = new ZkHosts( zkConnString, brokerZkPath );

    // Build configuration instance for Spout
    SpoutConfig spoutConfig = new SpoutConfig( hosts, topicName, zkRoot + "/" + topicName , UUID.randomUUID().toString() );

    // Build Multischeme instance
    spoutConfig.scheme = new SchemeAsMultiScheme( new StringScheme() );

    // Custom spout configuration
    spoutConfig.ignoreZkOffsets = true;

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

    /* ****************************************************************** */
    /* Set up topology                                                    */
    /* ****************************************************************** */
    /* Add Spout                                                          */
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout( "kafka-spout" , kafkaSpout, spoutParalismHint );

    // Add Bolt (Kafka Extractor)
    builder
      .setBolt("kafka-extractor", new KafkaExtractor(), boltParalismHint)
      .shuffleGrouping( "kafka-spout" );

    // Add Bolt (Elasticsearch Indexer)
    builder
      .setBolt("es-indexer", new ElasticsearchIndexer(), boltParalismHint)
      .fieldsGrouping("kafka-extractor", new Fields("id", "index", "type", "source"));

    // Finalize the topology
    StormTopology topology = builder.createTopology();

    // Exception message will go to standard output
    System.out.println("Cluster mode : " + args[0]);

    if ( args[0].equals( "local" ) ) {
      try {
        submitLocalCluster( conf, topology );
      } catch ( InterruptedException e ) {
        System.out.println("InterruptedException: " + e );
      }

    } else if ( args[0].equals( "prod" ) ) {
      submitProductionCluster( conf, topology );

    } else {
      System.out.println("Wrong option!");
      System.exit(0);
    }
  }

  private static void submitLocalCluster ( Config conf, StormTopology topology )
    throws InterruptedException {
    // Submit topology to local cluster
    // Set Debug Option
    conf.setDebug(true);

    // Create and execute object for local cluster
    LocalCluster cluster = new LocalCluster();

    cluster.submitTopology( topologyName, conf, topology );

    Utils.sleep(60000);

    // Shutdown local cluster gracefully
    cluster.shutdown();
  }

  private static void submitProductionCluster( Config conf, StormTopology topology ) {
    try {
      // Submit topology to producton cluster
      StormSubmitter.submitTopology( topologyName, conf, topology );

    } catch ( AlreadyAliveException e ) {
      System.out.println( "AlreadyAliveException : " + e);
    } catch ( InvalidTopologyException e ) {
      System.out.println( "InvalidTopologyException : " + e);
    } catch ( AuthorizationException e ) {
      System.out.println( "AuthorizatioinException : " + e);
    }
  }
}

