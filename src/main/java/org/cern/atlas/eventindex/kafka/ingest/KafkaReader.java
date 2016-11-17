package org.cern.atlas.eventindex.kafka.ingest;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.commons.codec.DecoderException;
//import org.apache.commons.codec.binary.Hex;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Arrays;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KafkaReader
{
 private static final String BROKERS = System.getProperty(
      "kafkaBrokers", "habench101:9092");


  private static final int REPORT_FREQ = Integer.parseInt(System.getProperty(
      "reportFrequency", "0"));

  private static final int BATCH_SIZE = Integer.parseInt(System.getProperty(
      "batchSize", "100"));

  private static final String AVRO_SCHEMA = System.getProperty(
      "avroSchema", "nofile");

  private static final String ZOOKEEPER = System.getProperty(
      "kafkaBrokers", "habench001:2181");

 private static final String GROUP_ID = System.getProperty(
      "groupId", "atlas_consumer");

  private static final String TOPIC = System.getProperty(
      "kafkaTopic", "atlas_ei");




  public static void main(String[] args) {

    //STATS
    long cumulativeFlush=0;
    long cumulativeParse=0;
    long cumulativeInsert=0;

    long startFlush=0;
    long startParse=0;
    long startInsert=0;


	
    //Init kafka consumer
    KafkaConsumer<String, byte[]> consumer=null;
    Properties props = new Properties();
    props.put("group.id",GROUP_ID);
//    props.put("metadata.broker.list", ZOOKEEPER);
    props.put("request.required.acks", "1");
//    props.put("serializer.class", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("bootstrap.servers", BROKERS);
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");



    Schema schema=null;
	
    try{
	  
                consumer = new KafkaConsumer<>(props);



		schema = new Schema.Parser().parse(new File(AVRO_SCHEMA));


        } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
        }


     int i=0;


     try{	
	   int timeouts = 0;
           consumer.subscribe(Arrays.asList(TOPIC));
	   while (true) {
		ConsumerRecords<String, byte[]> records = consumer.poll(200);
		if (records.count() == 0) {
                	timeouts++;
            	} else {
                	System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                	timeouts = 0;
            	}
		for (ConsumerRecord<String, byte[]> record : records) {
			byte[] received_message = record.value();
			DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
	                Decoder decoder = DecoderFactory.get().binaryDecoder(received_message, null);
	                GenericRecord payload2 = null;
	                payload2 = reader.read(null, decoder);
	                System.out.println("Message received : " + payload2);
		}

	  }
        }
	catch(Exception e)
        {
     		 e.printStackTrace();
        	System.exit(1);
    	} finally {
      	try {
    	          consumer.close();

        } catch (Exception e) {
          e.printStackTrace();
        }

    }


   }
}
