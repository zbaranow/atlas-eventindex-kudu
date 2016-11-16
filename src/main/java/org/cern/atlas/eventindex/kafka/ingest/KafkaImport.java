package org.atlas.eventindex.kafka.ingest;


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


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.IOException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import java.io.InputStream;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



public class KafkaImport
{

  private static final int REPORT_FREQ = Integer.parseInt(System.getProperty(
      "reportFrequency", "0"));

  private static final int BATCH_SIZE = Integer.parseInt(System.getProperty(
      "batchSize", "100"));

  private static final String AVRO_SCHEMA = System.getProperty(
      "avroSchema", "nofile");

  private static final String BROKERS = System.getProperty(
      "kafkaBrokers", "habench101:9092");

  private static final String TOPIC = System.getProperty(
      "kafkaTopic", "atlas_ei");



  private static final String SOURCE_FILE = System.getProperty(
      "file", "nofile");

  public static void main(String[] args) {

    //STATS
    long cumulativeFlush=0;
    long cumulativeParse=0;
    long cumulativeInsert=0;

    long startFlush=0;
    long startParse=0;
    long startInsert=0;


	
    //Opening a map file

    FileSystem fs = null;
    MapFile.Reader reader = null;
    Text txtValue = new Text();
    WritableComparable key = null;


    String[] keyParts=null;
    String[] valueParts=null;
    long startTime = System.currentTimeMillis();
    int col=0;


    //Init kafka producer
    KafkaProducer<String, byte[]> producer;
    Properties props = new Properties();
    props.put("bootstrap.servers", BROKERS);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

    producer = new KafkaProducer<>(props);

    Schema schema=null;
/*
    try (InputStream props = Resources.getResource("producer.props").openStream()) {
        Properties properties = new Properties();
        properties.load(props);
        producer = new KafkaProducer<>(properties);
    }
  */  

    //
    try{


                Configuration conf = new Configuration();


                conf.addResource(new Path("/afs/cern.ch/user/z/zbaranow/hadoop/clusters/lxhadoop/etc/hadoop/conf/core-site.xml"));
                conf.addResource(new Path("/afs/cern.ch/user/z/zbaranow/hadoop/clusters/lxhadoop/etc/hadoop/conf/hdfs-site.xml"));


                fs = FileSystem.get(conf);

                System.out.println("Reading "+ SOURCE_FILE);
                reader = new MapFile.Reader(fs, SOURCE_FILE, conf);


		schema = new Schema.Parser().parse(new File(AVRO_SCHEMA));


        } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
        }


     int i=0;


     try{	


	key = (WritableComparable)reader.getKeyClass().newInstance();
	while(reader.next(key,txtValue)){
             if (startParse!=0)
                     cumulativeParse+=System.currentTimeMillis()-startParse;


		GenericRecord row = new GenericData.Record(schema);

                //runnumber and eventnumber :format 00299680-00000676949
                startParse=System.currentTimeMillis();
                keyParts=key.toString().split("-"); //EXCEPTION SHOULD BE HANDLED
		row.put("runnumber",keyParts[0]);
		row.put("eventnumber",keyParts[1]);
                cumulativeParse+=System.currentTimeMillis()-startParse;

                startInsert=System.currentTimeMillis();
		
                cumulativeInsert+=System.currentTimeMillis()-startInsert;



                //schema of the rest: LumiBlockN=int BunchId=int EventTime=int EventTimeNanoSec=int EventWeight=float McChannelNumber=int, Lvl1ID=String IsSimulation=int IsCalibration=int IsTestBeam=int L1trigMask=String L2trigMask=String EFtrigMask=String SMK=int HLTPSK=int L1PSK=int nam0=String db0=String cnt0=String clid0=String tech0=String oid0=String nam1=String db1=String cnt1=String clid1=String tech1=String oid1=String nam2=String db2=String cnt2=String clid2=String tech2=String oid2=String nam3=String db3=String cnt3=String clid3=String tech3=String oid3=String

                startParse=System.currentTimeMillis();
                valueParts=txtValue.toString().split(",",-1);
                cumulativeParse+=System.currentTimeMillis()-startParse;

                if (valueParts.length!=54)
                {
                        System.out.println("Something wrong is with: "+txtValue.toString());
                        for (String s: valueParts) System.out.println(s);

                }
						row.put("lumiblockn",Long.parseLong(valueParts[col++]));
                row.put("bunchid",Long.parseLong(valueParts[col++]));
                row.put("eventtime",Long.parseLong(valueParts[col++]));
                row.put("eventtimenanosec",Long.parseLong(valueParts[col++]));
                row.put("eventweight",Float.parseFloat(valueParts[col++]));
                row.put("mcchannelnumber",Long.parseLong(valueParts[col++]));
                row.put("lvl1id",valueParts[col++]);
                row.put("issimulation",Long.parseLong(valueParts[col++]));
                row.put("iscalibration",Long.parseLong(valueParts[col++]));
                row.put("istestbeam",Long.parseLong(valueParts[col++]));
                row.put("l1trigmask",valueParts[col++]);
                row.put("l2trigmask",valueParts[col++]);
                row.put("eftrigmask",valueParts[col++]);
                row.put("smk",Long.parseLong(valueParts[col++]));
                row.put("hltpsk",Long.parseLong(valueParts[col++]));
                row.put("l1psk",Long.parseLong(valueParts[col++]));
                row.put("nam0",valueParts[col++]);
                row.put("db0",valueParts[col++]);
                row.put("cnt0",valueParts[col++]);
                row.put("clid0",valueParts[col++]);
                row.put("tech0",valueParts[col++]);//20
                row.put("oid0",valueParts[col++]);
                row.put("nam1",valueParts[col++]);
                row.put("db1",valueParts[col++]);
                row.put("cnt1",valueParts[col++]);
                row.put("clid1",valueParts[col++]);
                row.put("tech1",valueParts[col++]);
                row.put("oid1",valueParts[col++]);
                row.put("nam2",valueParts[col++]);
                row.put("db2",valueParts[col++]);
                row.put("cnt2",valueParts[col++]);
                row.put("clid2",valueParts[col++]);
                row.put("tech2",valueParts[col++]);
                row.put("oid2",valueParts[col++]);
                row.put("nam3",valueParts[col++]);
                row.put("db3",valueParts[col++]);
                row.put("cnt3",valueParts[col++]);
                row.put("clid3",valueParts[col++]);
                row.put("tech3",valueParts[col++]);
                row.put("oid3",valueParts[col++]);
                row.put("project",valueParts[col++]);
                row.put("streamname",valueParts[col++]);
                row.put("prodstep",valueParts[col++]);
                row.put("datatype",valueParts[col++]);
                row.put("amitag",valueParts[col++]);
                row.put("l1trigchainstav",valueParts[col++]);
                row.put("l1trigchainstap",valueParts[col++]);
                row.put("l1trigchainstbp",valueParts[col++]);
                row.put("l2trigchainsph",valueParts[col++]);
                row.put("l2trigchainspt",valueParts[col++]);
                row.put("l2trigchainsrs",valueParts[col++]);//50
                row.put("eftrigchainsph",valueParts[col++]);
                row.put("eftrigchainspt",valueParts[col++]);
                row.put("eftrigchainsrs",valueParts[col++]);//53
	

		//Serializing Avro to bytes
		DatumWriter<GenericRecord> writer = new SpecificDatumWriter<GenericRecord>(schema);
	        ByteArrayOutputStream out = new ByteArrayOutputStream();
	        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
	        writer.write(row, encoder);
	        encoder.flush();
	        out.close();
		byte[] serializedBytes = out.toByteArray();


		//Sending a message

	        ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(TOPIC, serializedBytes);
	        producer.send(message);

		i++;
                if ((i % BATCH_SIZE) == 0 )
                {
                  //flush session
                  startFlush=System.currentTimeMillis();
                  producer.flush();
                  cumulativeFlush+=System.currentTimeMillis()-startFlush;
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

                }

                startParse=System.currentTimeMillis();




	  }
        }
	catch(Exception e)
        {
     		 e.printStackTrace();
        	System.exit(1);
    	} finally {
      	try {
    	          producer.close();

        } catch (Exception e) {
          e.printStackTrace();
        }

    }


   }
}
