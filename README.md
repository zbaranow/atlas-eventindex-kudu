ATLAS EventIndex Kudu tools
==============
This project is a simple implementation of import and query tools for using the
main functionalities of EI with Apache Kudu backend

Compililing the project
--------------
The easiest way to compile is to use <b> Maven</b>, POM file is provided.
To compile with Maven just run:
```
mvn package
```

Creating a Kudu table
--------------
<b>CreateEITable.class</b> creates a table with proper stucture (columns and 
paritiotns) and encoding and compression algorithms
In order to run the class use you have to specify
- kudu master host
- target Kudu table name

###### Example

```
java -DkuduMaster=haperf100 -DtableName=atlas_event_index -cp target/atlas-eventindex-kudu-1.0-SNAPSHOT.jar org.cern.atlas.eventindex.kudu.ingest.CreateEITable
```

Importing a dataset from a mapfile
--------------
<b>KuduImport.class</b> imports a specified file from HDFS into a Kudu table.
To be executed on a machine with a target Hadoop cluster configuration and libs installed.
(like one of a Hadoop cluster nodes).
The following parameters have to be specified:
- kudu master host
- target Kudu table name
- map file path to be imported

###### Example

```
java -DkuduMaster=haperf100 -DtableName=atlas_eventindex -Dfile=/user/atlevind/EI16.1/data16_cos.00299680.physics_Late.merge.AOD.f703_m1600 -cp \$(hadoop classpath):./atlas-eventindex-kudu-1.0-SNAPSHOT.jar org.cern.atlas.eventindex.kudu.ingest.KuduImport
```


Event picking
--------------
<b>EI.class</b> queries for a specified runnumber and eventnumber pair and returns corresponding GUID.
Parameters to be specified:
- kudu master host
- kudu table to query from
- runnumber 
- eventnumber

###### Example1

```
java -DkuduMaster=haperf100 -DtableName=atlas_eventindex -cp ./target/atlas-eventindex-kudu-1.0-SNAPSHOT.jar org.cern.atlas.eventindex.kudu.query.EI 263965
```

Optionally list of columns to be returned (beside GUID) can be spcified as a space
separated string with <b>-Dcolumns</b> option (for all colummns specify "all").

###### Example2

```
java -DkuduMaster=haperf100 -DtableName=atlas_eventindex  -Dcolumns="all" -cp ./target/atlas-eventindex-kudu-1.0-SNAPSHOT.jar org.cern.atlas.eventindex.kudu.query.EI 263965
```

It is also possibility to query multiple runnumber and eventnumber pairs by
sourcing them from a file with option <b>-Dfile</b>

###### Example3

```
java -DkuduMaster=haperf100 -DtableName=atlas_eventindex  -Dcolumns="all" -Dfile=./picklist.txt -cp ./target/atlas-eventindex-kudu-1.0-SNAPSHOT.jar org.cern.atlas.eventindex.kudu.query.EI
```



Importing a dataset from a mapfile via Kafka
--------------
Alternative way to direct write to a Kudu instance is to push data via Kafka cluster. 
This will improve relaiability and scalability of a ingestion pipes at a price of complexity - a Kafka cluster is needed.
In such case to type of process are needed (that can be parallelized): 
- one to read a mapfile and store it on Kafka - impemented by <b>KafkaImport.class</b>
- second to read the data from Kafka and put it into Kudu - implemented by <b>KafkaKuduImport.class</b>

In order to import data from a MapFile stored on HDFS into Kafka the following parameters has to be specified
- <b>file</b> - HDFS path to a mapfile
- <b>kafkaBrokers</b> - List (not necessery complete) of Kafka brokers
- <b>kafkaTopic</b> - Topic on a Kafka cluster to which the data will be serialized
- <b>avroSchema</b> - An avro schema file to be used for data serialization

###### Example 1
```
java -DavroSchema=./atlas_ei.avsc -DkafkaTopic=atlas_ei -DreportFrequency=50000 -DkafkaBrokers="kafka1:9092 kafka2:9092"  -Dfile=/user/atlevind/EI16.1/data16_cos.00299680.physics_Late.merge.AOD.f703_m1600 -DbatchSize=1000 -cp \$(hadoop classpath):./atlas-eventindex-kudu-1.0-SNAPSHOT.jar
```

In order to import data from a Kafka topic into Kudu the following parameters has to be specified
- <b>kuduMaster</b> - Kudu master server name
- <b>tableName</b> - Kudu master server name
- <b>kafkaBrokers</b> - List (not necessery complete) of Kafka brokers
- <b>kafkaTopic</b> - Topic on a Kafka cluster from which data should be deserialized
- <b>avroSchema</b> - An avro schema file to be used for data serialization



###### Example 2
`java -DreportFrequency=50000 -DkuduMaster=itrac1508 -DtableName=atlas_event_index -DbatchSize=100 -DkafkaTopic=atlas_ei -DavroSchema=atlas_ei.avsc -cp target/atlas-eventindex-kudu-1.0-SNAPSHOT.jar org.cern.atlas.eventindex.kudu.ingest.KafkaKuduImport`

