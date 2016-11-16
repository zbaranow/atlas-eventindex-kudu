package org.cern.atlas.eventindex.kudu.query;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.lang.Integer;
import java.io.BufferedReader;
import java.io.FileReader;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;
import java.util.logging.LogManager;



public class EI {

  private static final String KUDU_MASTER = System.getProperty(
      "kuduMaster", "null");

  private static final String TABLE_NAME = System.getProperty(
      "tableName", "KuduTable");


 private static final String SOURCE_FILE = System.getProperty(
      "file", "nofile");


  private static final String COLUMNS = System.getProperty(
      "columns", "runnumber eventnumber db0");

  private static long count=0;

  public static void main(String[] args) {

    KuduClient client=null;

    try
    {
	//establishing connection
        client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
	KuduTable table = client.openTable(TABLE_NAME);


	String[] columns = COLUMNS.split(" ");


	//TABLE SCANNER

	long startTime = System.currentTimeMillis();

	   if (SOURCE_FILE.equals("nofile"))
	   {
		KuduScannerBuilder scannerBuilder =  client.newScannerBuilder(table);
                if (!COLUMNS.equals("all")) scannerBuilder.setProjectedColumnNames((List<String>)Arrays.asList(columns));

		scan(scannerBuilder,Long.parseLong(args[0]),Long.parseLong(args[1]));
	   }
	   else{
		BufferedReader br = new BufferedReader(new FileReader(SOURCE_FILE));
		for(String line; (line = br.readLine()) != null; ) {

			String[] pk = line.split(" ");
			KuduScannerBuilder scannerBuilder =  client.newScannerBuilder(table);
			if (!COLUMNS.equals("all")) scannerBuilder.setProjectedColumnNames((List<String>)Arrays.asList(columns));

	                scan(scannerBuilder,Long.parseLong(pk[0]),Long.parseLong(pk[1]));

    		}
	   }
	    
	System.out.println(count+" requests in serial took "+(System.currentTimeMillis()-startTime)+" ms, avg time per request "+(System.currentTimeMillis()-startTime)/(float)count+" ms"  );

	
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

  static void scan(KuduScannerBuilder sb, long runnumber, long eventnumber) throws KuduException
  {
	//System.out.println(runnumber+" "+eventnumber);
	
	KuduPredicate predRun = KuduPredicate.newComparisonPredicate(
                new ColumnSchemaBuilder("runnumber", Type.INT64).build(),
                KuduPredicate.ComparisonOp.EQUAL, runnumber);

        KuduPredicate predEvent = KuduPredicate.newComparisonPredicate(
                new ColumnSchemaBuilder("eventnumber", Type.INT64).build(),
                KuduPredicate.ComparisonOp.EQUAL, eventnumber);

	sb.addPredicate(predRun).addPredicate(predEvent);

        KuduScanner scanner =sb.build();


	while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                  RowResult result = results.next();
                        System.out.println((count++)+": "+result.rowToString());
		}
        }


  }
}

