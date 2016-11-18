package org.cern.atlas.eventindex.kudu.ingest;




import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import org.apache.kudu.ColumnSchema;
//import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.SessionConfiguration.FlushMode;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.lang.Integer;
import java.util.Properties;
import java.io.File;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;


public class KafkaKuduImport {

  private static final String KUDU_MASTER = System.getProperty(
      "kuduMaster", "null");

  private static final String TABLE_NAME = System.getProperty(
      "tableName", "KuduTable");


 private static final int BATCH_SIZE = Integer.parseInt(System.getProperty(
      "batchSize", "500"));

 private static final int REPORT_FREQ = Integer.parseInt(System.getProperty(
      "reportFrequency", "0"));

private static final String BROKERS = System.getProperty(
      "kafkaBrokers", "habench101:9092");

 private static final String AVRO_SCHEMA = System.getProperty(
      "avroSchema", "nofile");

 private static final String GROUP_ID = System.getProperty(
      "groupId", "atlas_consumer");

  private static final String TOPIC = System.getProperty(
      "kafkaTopic", "atlas_test");



 private static final int TIMEOUT = Integer.parseInt(System.getProperty(
      "timeout", "1000"));


  public static void main(String[] args) {


    //STATS
    long cumulativeFlush=0;
    long cumulativeParse=0;
    long cumulativeInsert=0;

    long startFlush=0;
    long startParse=0;
    long startInsert=0;
  
    //Opening a map file
    

	



    KuduClient client=null;
    Schema schema = null;

    try
    {
	//establishing connection
        client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
	KuduTable table = client.openTable(TABLE_NAME);
	KuduSession session = client.newSession();

        //session config
	session.setIgnoreAllDuplicateRows(true);
	session.setMutationBufferSpace(BATCH_SIZE*2);
	session.setFlushMode(FlushMode.MANUAL_FLUSH);



	//Init kafka consumer
        KafkaConsumer<String, byte[]> consumer=null;
        Properties props = new Properties();
        props.put("group.id",GROUP_ID);
        props.put("enable.auto.commit", "false");
        props.put("request.required.acks", "1");
        props.put("bootstrap.servers", BROKERS);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(TOPIC));


	//avro schema
        schema = new Schema.Parser().parse(new File(AVRO_SCHEMA));



	
    	long i=0; //rows counter
	Insert insert = null;
	PartialRow row = null;

        
	String[] keyParts=null;
	String[] valueParts=null;
        long startTime = System.currentTimeMillis();
	int col=0;
	int timeouts=0;

        GenericRecord grecord = null;
	Decoder decoder = null;
	DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
	byte[] received_message = null;

        boolean loop=true;

	while (loop) {
	     ConsumerRecords<String, byte[]> records = consumer.poll(TIMEOUT);
             System.out.println(records.count());
	     if (startParse!=0)
	             cumulativeParse+=System.currentTimeMillis()-startParse;
	     if (records.count() == 0) {
                       timeouts++;
             } else {
                 //System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
                 timeouts = 0;
             }
             for (ConsumerRecord<String, byte[]> record : records) {
		 if (i==0) System.out.println("Started at "+record.offset());
                 startParse=System.currentTimeMillis();

                 received_message = record.value();
                 decoder = DecoderFactory.get().binaryDecoder(received_message, null);
                 grecord = reader.read(null, decoder);

		 cumulativeParse+=System.currentTimeMillis()-startParse;

	         insert = table.newInsert();
        	 row = insert.getRow();

                 startInsert=System.currentTimeMillis();
		 for(Schema.Field field: schema.getFields())
		 {
			
			Schema.Type type = field.schema().getTypes().get(1).getType();
//			System.out.println(type+" "+field.name()+" "+grecord.get(field.name()));
			if (type.equals(Schema.Type.STRING))
			{
				row.addString(field.name(), grecord.get(field.name()).toString() ) ;
			} else if (type.equals(Schema.Type.DOUBLE))
			{
				row.addDouble(field.name(), (Double)(grecord.get(field.name())));
			} else if (type.equals(Schema.Type.INT))
			{
				row.addInt(field.name(), (Integer)(grecord.get(field.name())));
			} else if (type.equals(Schema.Type.LONG))
			{
				row.addLong(field.name(), (Long)(grecord.get(field.name())));
			} else if (type.equals(Schema.Type.FLOAT))
			{
				row.addFloat(field.name(), (Float)(grecord.get(field.name())));
			} else
			{
		//		throw new RuntimeException("ConvertEnvMultiTable2MultiAvro doesn't supper type :" + type + " yet.  Put in a bug request");
			}
		 }//for each field

		try
		{
	         session.apply(insert);

		}
		catch(Exception e)
		{
			System.out.println(grecord);
			throw e;
		}
                 cumulativeInsert+=System.currentTimeMillis()-startInsert;
		 i++;
		if ((i % BATCH_SIZE) == 0 )
                {

                   //flush session
                   startFlush=System.currentTimeMillis();
                   session.flush();
                   cumulativeFlush+=System.currentTimeMillis()-startFlush;
                   consumer.commitSync();
		   System.out.println("Commiting after "+i+" at "+ record.offset());

                }


		 if (REPORT_FREQ>0 && (i % REPORT_FREQ) == 0 )
	        {
	                //Print single line report
	                long ctime=System.currentTimeMillis();
	                System.out.println(i+" rows inserted in "+(ctime-startTime)+
	                " ms (avg: "+String.format("%.2f",(i*1000)/(float)(ctime-startTime))+" rows/s),"+
	                " P: "+cumulativeParse+
	                " I: "+cumulativeInsert+
	                " F: "+cumulativeFlush);

        	}//if report

             }//for each message



		
	     consumer.commitSync();

	     startParse=System.currentTimeMillis();
        }//while loop

	//final flush
       	startFlush=System.currentTimeMillis();
        session.flush();
        cumulativeFlush+=System.currentTimeMillis()-startFlush;
        consumer.commitSync();


	
        long endTime = System.currentTimeMillis();

	System.out.println(i+" rows inserted in "+(endTime-startTime)+" ms (avg: "+String.format("%.2f",(i*1000)/(float)(endTime-startTime))+" rows/s)");
	
	//Time profile
	
	System.out.println("Time profile:");
        System.out.println("   time spent on decoding MF: "+cumulativeParse+ "ms");
        System.out.println("   time spent on inserting data to buffers: "+cumulativeInsert+ "ms");
        System.out.println("   time spent on flushing data to Kudu: "+cumulativeFlush+ "ms");
    }//try
    catch(Exception e)
    {
      e.printStackTrace();
	System.exit(1);
    } finally {
      try {
          client.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      
    }
  }
}

