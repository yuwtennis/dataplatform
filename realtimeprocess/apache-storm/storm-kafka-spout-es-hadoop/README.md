## GOAL

Goal of this project is to read data from *apache kafka* and insert data into *elasticsearch*

## ENVIRONMENT

|Components|Version|
|----------|-------|
|apache maven|3.5.3|
|apache storm|1.2.1|
|apache kafka|2.12-1.1.0|
|elasticsearch|6.2.4|
|es-hadoop|6.2.4|

## CONCEPT

*Apache storm* reads data from *apache kafka* using *kafka spout* . Then in **bolt2** , tuple will be converted into json format which will be indexed into elasticsearch. 

I have used *es-hadoop* in this project. 

![img](https://github.com/yuwtennis/dataplatform/blob/master/realtimeprocess/apache-storm/storm-kafka-spout-es-hadoop/img/basic-architecture.png)

For data modeling, I have mapped the index name same name as topic name in apache kafka. *system.properties* will let you change the *topic name* and *index name*.

![img](https://github.com/yuwtennis/dataplatform/blob/master/realtimeprocess/apache-storm/storm-kafka-spout-es-hadoop/img/simple-datamodeling.png)

## HOWTO

Below is the instruction to use the environment.

### Using local cluster

1. Prepare apache kafka environment
1. Prepare apache storm environment
2. Download the sourcecode.
3. Change directory to below directory.
  cd storm-kafka-spout-es-hadoop
4. Adjust the *resources/system.properties*.   - 
4. mvn package
5. Startup the cluster.
  [YOUR STORM ENVIRONMENT]/bin/storm jar target/storm-kafka-spout-es-hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar proj.storm.kafka.spout.Topology local system.properties
  
### Using production cluster
Use below command instead.

 [YOUR STORM ENVIRONMENT]/bin/storm jar target/storm-kafka-spout-es-hadoop-1.0-SNAPSHOT-jar-with-dependencies.jar proj.storm.kafka.spout.Topology prod system.properties
 
 ## TO-DO
 Elaborate more on *system.properties*.
