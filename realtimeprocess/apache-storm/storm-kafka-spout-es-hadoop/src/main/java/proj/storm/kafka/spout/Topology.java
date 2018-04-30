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
import org.apache.storm.LocalCluster;
import org.elasticsearch.storm.EsBolt;
import java.lang.InterruptedException;
import java.util.UUID;
import java.util.Properties;
import java.io.IOException;

import proj.storm.kafka.spout.EsJsonTransfomer;

// Define topology
public class Topology {
    // Private class variables may be shared among the methods
    /*
     * Topology configuration variables : Zookeeper connection parameter
     */
    private static String ES_WRITE_ACK;
    private static int ES_FLUSH_ENTRIES_SIZE;
    private static String ES_TICK_TUPLE_FLUSH;
    private static String ES_TARGET;
    private static String ES_INDEX;
    private static String ES_TYPE;

    /*
     * Topology configuration variables : Zookeeper connection parameter
     */
    private static String ZK_CONNSTRING;
    private static String BROKER_ZK_PATH;
    private static String ZK_ROOT;
    private static String TOPIC_NAME;

    /*
     * Topology configuration variables : Storm topology specific parameter
     */
    private static String TOPOLOGY_NAME;
    private static int WORKERS;
    private static int ACKER_EXECUTORS;
    private static int MAX_SPOUT_PENDING;
    private static int MESSAGE_TIMEOUT_SECS;
    private static int TICK_TUPLE_FREQ_SECS;

    /*
     * Topology configuration variables : Storm spout specific parameter
     */
    private static String SPOUT_NAME;
    private static int SPOUT_PARALISM_HINT;

    /*
     * Topology configuration variables : Storm bolt specific parameter
     */
    private static String BOLT_NAME_1;
    private static String BOLT_NAME_2;
    private static int BOLT_PARALISM_HINT_1;
    private static int BOLT_PARALISM_HINT_2;

    /*
     * Main
     */ 
    public static void main( String[] args ) {
      // Do argument check first
      argCheck( args );

      // Load all the properties
      loadProp( args[1] );

      /*
       * Build kafka consumer spout
       */
      // Build zookeeper instance
      BrokerHosts hosts = new ZkHosts( ZK_CONNSTRING, BROKER_ZK_PATH );
  
      // Build configuration instance for Spout
      SpoutConfig spoutConfig = new SpoutConfig( hosts, TOPIC_NAME, ZK_ROOT + "/" + TOPIC_NAME , UUID.randomUUID().toString() );
  
      // Build Multischeme instance
      spoutConfig.scheme = new SchemeAsMultiScheme( new StringScheme() );
  
      // Custom spout configuration
      spoutConfig.ignoreZkOffsets = true;
  
      // Build Kafka spout
      KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
  
      /*
       * Set up storm configuration
       * Based on common config parameters
       * http://storm.apache.org/releases/1.2.1/Running-topologies-on-a-production-cluster.html
       */
  
      Config conf = new Config();
  
      // Config.TOPOLOGY_WORKERS
      conf.setNumWorkers(WORKERS);
  
      // Config.TOPOLOGY_ACKER_EXECUTORS
      conf.setNumAckers(ACKER_EXECUTORS);
  
      // Config.TOPOLOGY_MAX_SPOUT_PENDING
      conf.setMaxSpoutPending(MAX_SPOUT_PENDING);
  
      // Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS
      // Default
      conf.setMessageTimeoutSecs(MESSAGE_TIMEOUT_SECS);
  
      /*
       * EsBolt configuration
       */
      conf.put("es.storm.bolt.write.ack", ES_WRITE_ACK);
      conf.put("es.storm.bolt.flush.entries.size", ES_FLUSH_ENTRIES_SIZE);
      conf.put("es.storm.bolt.tick.tuple.flush", ES_TICK_TUPLE_FLUSH);
  
      /*
       * Set up topology
       */
      // Add Spout
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout( SPOUT_NAME , kafkaSpout, SPOUT_PARALISM_HINT );
  
      // Add Bolt (Kafka Extractor)
      builder
        .setBolt(BOLT_NAME_1, new EsJsonTransfomer(), BOLT_PARALISM_HINT_1 )
        .shuffleGrouping( SPOUT_NAME );
  
      // Add Bolt (Elasticsearch Indexer)
      builder
        .setBolt(BOLT_NAME_2, new EsBolt(ES_INDEX + "/" + ES_TYPE), BOLT_PARALISM_HINT_2 )
        .addConfiguration(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_TUPLE_FREQ_SECS)
        .fieldsGrouping( BOLT_NAME_1, new Fields( ES_TARGET) );
  
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
  
      }
    }
  
    private static void submitLocalCluster ( Config conf, StormTopology topology )
      throws InterruptedException {
        // Submit topology to local cluster
        // Set Debug Option
        conf.setDebug(true);
  
        // Create and execute object for local cluster
        LocalCluster cluster = new LocalCluster();
  
        cluster.submitTopology( TOPOLOGY_NAME, conf, topology );
  
        Utils.sleep(60000);
  
        // Shutdown local cluster gracefully
        cluster.shutdown();
    }
  
    private static void submitProductionCluster( Config conf, StormTopology topology ) {
        try {
          // Submit topology to producton cluster
          StormSubmitter.submitTopology( TOPOLOGY_NAME, conf, topology );
  
        } catch ( AlreadyAliveException e ) {
          System.out.println( "AlreadyAliveException : " + e);
        } catch ( InvalidTopologyException e ) {
          System.out.println( "InvalidTopologyException : " + e);
        } catch ( AuthorizationException e ) {
          System.out.println( "AuthorizatioinException : " + e);
        }
    }

    private static void argCheck( String[] args ) {
        // If nothing is specified quit
        if ( args.length == 0 ) {
            System.out.println("Nothing is specified. Quitting...");
            System.exit(0);
        }
         
        // Check if the input argv includes the storm cluster mode
        if ( ! args[0].equals( "prop" ) && ! args[0].equals( "local" ) ) {
            System.out.println("Wrong cluster mode. Quitting...");
            System.exit(0);
        }

        // Check if the input argv includes the properties file name
        if ( args[1] == null ) {
            System.out.println("No property file specified. Quitting");
            System.exit(0);
        }
    }

    private static void loadProp ( String propFile ) {
        try {
            // Create Properties class object
            Properties property = new Properties();
  
            // Load the lines from using inputstream
            property.load( proj.storm.kafka.spout.Topology.class.getClassLoader().getResourceAsStream( propFile ) );

            // EsBolt
            ES_WRITE_ACK          = property.getProperty("es.bolt.write.ack");
            ES_FLUSH_ENTRIES_SIZE = Integer.parseInt( property.getProperty("es.bolt.flush.entries.size") );
            ES_TICK_TUPLE_FLUSH   = property.getProperty("es.bolt.tick.tuple.flush");
            ES_TARGET             = property.getProperty("es.bolt.target");
            ES_INDEX              = property.getProperty("es.bolt.index.name");
            ES_TYPE               = property.getProperty("es.bolt.index.type");
    
            // Zookeeper
            ZK_CONNSTRING         = property.getProperty("zookeeper.host");
            BROKER_ZK_PATH        = property.getProperty("zookeeper.brokerpath");
            ZK_ROOT               = property.getProperty("zookeeper.root"); 
            TOPIC_NAME            = property.getProperty("zookeeper.topic.name");
    
            // Storm topology general
            TOPOLOGY_NAME         = property.getProperty("storm.topology.name");
            WORKERS               = Integer.parseInt( property.getProperty("storm.topology.workers") );
            ACKER_EXECUTORS       = Integer.parseInt( property.getProperty("storm.topology.acker_executors") );
            MAX_SPOUT_PENDING     = Integer.parseInt( property.getProperty("storm.topology.max_spout_pending") );
            MESSAGE_TIMEOUT_SECS  = Integer.parseInt( property.getProperty("storm.topology.message_timeout_secs") );
            TICK_TUPLE_FREQ_SECS  = Integer.parseInt( property.getProperty("storm.topology.tick_tuple_freq_secs") );
    
            // Storm spout specific
            SPOUT_NAME            = property.getProperty("storm.spout.name");
            SPOUT_PARALISM_HINT   = Integer.parseInt( property.getProperty("storm.spout.paralismhint") );
    
            // Storm bolt specific
            BOLT_NAME_1           = property.getProperty("storm.bolt.name.1");
            BOLT_NAME_2           = property.getProperty("storm.bolt.name.2");
            BOLT_PARALISM_HINT_1  = Integer.parseInt( property.getProperty("storm.bolt.paralismhint.1") );
            BOLT_PARALISM_HINT_2  = Integer.parseInt( property.getProperty("storm.bolt.paralismhint.2") );
        } catch ( IOException e ) {
            System.out.println("Property file does not exist. Quitting");
            System.exit(0);
        }
    }
}

