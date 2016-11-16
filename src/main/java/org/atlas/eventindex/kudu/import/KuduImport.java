package org.cern.atlas.eventindex.import.kudu;

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



import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.SessionConfiguration.FlushMode;


import java.util.ArrayList;
import java.util.List;
import java.lang.Integer;
//import java.lang.Math;

public class KuduImport {

  private static final String KUDU_MASTER = System.getProperty(
      "kuduMaster", "null");

  private static final String TABLE_NAME = System.getProperty(
      "tableName", "KuduTable");


 private static final String SOURCE_FILE = System.getProperty(
      "file", "nofile");

 private static final int BATCH_SIZE = Integer.parseInt(System.getProperty(
      "batchSize", "100"));

 private static final int REPORT_FREQ = Integer.parseInt(System.getProperty(
      "reportFrequency", "0"));



  public static void main(String[] args) {
    System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
    System.out.println("Run with -DkuduMaster=myHost:port to override.");
    System.out.println("Will import  "+ SOURCE_FILE+ " into "+TABLE_NAME+" table. Use -Dfile=path and -DtableName=tablename to override");
    System.out.println("-----------------------------------------------");


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

    try{
	
		Configuration conf = new Configuration();


		conf.addResource(new Path("/afs/cern.ch/user/z/zbaranow/hadoop/clusters/lxhadoop/etc/hadoop/conf/core-site.xml"));
        	conf.addResource(new Path("/afs/cern.ch/user/z/zbaranow/hadoop/clusters/lxhadoop/etc/hadoop/conf/hdfs-site.xml"));


                fs = FileSystem.get(conf);

		System.out.println("Reading "+ SOURCE_FILE);
		reader = new MapFile.Reader(fs, SOURCE_FILE, conf);


	} catch (IOException e) {
                e.printStackTrace();
		System.exit(1);
	}



    KuduClient client=null;

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

        System.out.println("Kudu session created");
	
    	long i=0; //rows counter
	Insert insert = null;
	PartialRow row = null;

        Text txtValue = new Text();
        WritableComparable key = (WritableComparable)reader.getKeyClass().newInstance();
        
	String[] keyParts=null;
	String[] valueParts=null;
        long startTime = System.currentTimeMillis();
	int col=0;

        while(reader.next(key,txtValue)){
	     if (startParse!=0)
	             cumulativeParse+=System.currentTimeMillis()-startParse;

	     //extracting data from mapfile and insert it into Kudu insert object
             insert = table.newInsert();

	     row = insert.getRow();


		
		//runnumber and eventnumber :format 00299680-00000676949
                startParse=System.currentTimeMillis();		
		keyParts=key.toString().split("-"); //EXCEPTION SHOULD BE HANDLED
                cumulativeParse+=System.currentTimeMillis()-startParse;

                startInsert=System.currentTimeMillis();
		row.addLong("runnumber",Long.parseLong(keyParts[0]));
                row.addLong("eventnumber",Long.parseLong(keyParts[1]));
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

		col=0;

                startInsert=System.currentTimeMillis();
		row.addLong("lumiblockn",Long.parseLong(valueParts[col++]));
                row.addLong("bunchid",Long.parseLong(valueParts[col++]));
                row.addLong("eventtime",Long.parseLong(valueParts[col++]));
                row.addLong("eventtimenanosec",Long.parseLong(valueParts[col++]));
                row.addFloat("eventweight",Float.parseFloat(valueParts[col++]));
                row.addLong("mcchannelnumber",Long.parseLong(valueParts[col++]));
                row.addString("lvl1id",valueParts[col++]);
                row.addLong("issimulation",Long.parseLong(valueParts[col++]));
                row.addLong("iscalibration",Long.parseLong(valueParts[col++]));
                row.addLong("istestbeam",Long.parseLong(valueParts[col++]));
                row.addString("l1trigmask",valueParts[col++]);
                row.addString("l2trigmask",valueParts[col++]);
                row.addString("eftrigmask",valueParts[col++]);
                row.addLong("smk",Long.parseLong(valueParts[col++]));
                row.addLong("hltpsk",Long.parseLong(valueParts[col++]));
                row.addLong("l1psk",Long.parseLong(valueParts[col++]));
                row.addString("nam0",valueParts[col++]);
                row.addString("db0",valueParts[col++]);
                row.addString("cnt0",valueParts[col++]);
                row.addString("clid0",valueParts[col++]);
                row.addString("tech0",valueParts[col++]);//20
                row.addString("oid0",valueParts[col++]);
                row.addString("nam1",valueParts[col++]);
                row.addString("db1",valueParts[col++]);
                row.addString("cnt1",valueParts[col++]);
                row.addString("clid1",valueParts[col++]);
                row.addString("tech1",valueParts[col++]);
                row.addString("oid1",valueParts[col++]);
                row.addString("nam2",valueParts[col++]);
                row.addString("db2",valueParts[col++]);
                row.addString("cnt2",valueParts[col++]);
                row.addString("clid2",valueParts[col++]);
                row.addString("tech2",valueParts[col++]);
                row.addString("oid2",valueParts[col++]);
                row.addString("nam3",valueParts[col++]);
                row.addString("db3",valueParts[col++]);
                row.addString("cnt3",valueParts[col++]);
                row.addString("clid3",valueParts[col++]);
                row.addString("tech3",valueParts[col++]);
                row.addString("oid3",valueParts[col++]);
                row.addString("project",valueParts[col++]);	
                row.addString("streamname",valueParts[col++]);
                row.addString("prodstep",valueParts[col++]);
                row.addString("datatype",valueParts[col++]);
                row.addString("amitag",valueParts[col++]);
                row.addString("l1trigchainstav",valueParts[col++]);
                row.addString("l1trigchainstap",valueParts[col++]);
                row.addString("l1trigchainstbp",valueParts[col++]);
                row.addString("l2trigchainsph",valueParts[col++]);
                row.addString("l2trigchainspt",valueParts[col++]);
                row.addString("l2trigchainsrs",valueParts[col++]);//50
                row.addString("eftrigchainsph",valueParts[col++]);
                row.addString("eftrigchainspt",valueParts[col++]);
                row.addString("eftrigchainsrs",valueParts[col++]);//53

		

             session.apply(insert);
	     cumulativeInsert+=System.currentTimeMillis()-startInsert;

             i++;
             if ((i % BATCH_SIZE) == 0 )
             {
		//flush session
		startFlush=System.currentTimeMillis();
		session.flush();
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
	if  ( (i % BATCH_SIZE) != 0)
	{
         	  startFlush=System.currentTimeMillis();
                session.flush();
                cumulativeFlush+=System.currentTimeMillis()-startFlush;

	}
        long endTime = System.currentTimeMillis();

	System.out.println(i+" rows inserted in "+(endTime-startTime)+" ms (avg: "+String.format("%.2f",(i*1000)/(float)(endTime-startTime))+" rows/s)");
	
	//Time profile
	
	System.out.println("Time profile:");
        System.out.println("   time spent on decoding MF: "+cumulativeParse+ "ms");
        System.out.println("   time spent on inserting data to buffers: "+cumulativeInsert+ "ms");
        System.out.println("   time spent on flushing data to Kudu: "+cumulativeFlush+ "ms");
    }
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

