package org.cern.atlas.eventindex.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.lang.Integer;

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



  public static void main(String[] args) {
/*    System.out.println("Connecting to " + KUDU_MASTER);
    System.out.println("Run with -DkuduMaster=myHost:port to override.");
    System.out.println("Will read from  "+TABLE_NAME+" table. Use -DtableName=tablename to override");
    if (SOURCE_FILE.equals("nofile"))
	    System.out.println("Will read arguments from console. Use -Dfile=path to override");
    else
	    System.out.println("Will read arguments from file "+SOURCE_FILE);

*/


    System.out.println("-----------------------------------------------");


	



    KuduClient client=null;

    try
    {
	//establishing connection
        client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();
	KuduTable table = client.openTable(TABLE_NAME);


	String[] columns = COLUMNS.split(" ");


	long runnumber=Long.parseLong(args[0]);
	long eventnumber=Long.parseLong(args[1]);

	KuduPredicate predRun = KuduPredicate.newComparisonPredicate(
		new ColumnSchemaBuilder("runnumber", Type.INT64).build(),
		KuduPredicate.ComparisonOp.EQUAL, runnumber);

	KuduPredicate predEvent = KuduPredicate.newComparisonPredicate(
                new ColumnSchemaBuilder("eventnumber", Type.INT64).build(),
                KuduPredicate.ComparisonOp.EQUAL, eventnumber);


	//TABLE SCANNER

	long startTime = System.currentTimeMillis();
	KuduScanner scanner = client.newScannerBuilder(table)
		.setProjectedColumnNames((List<String>)Arrays.asList(columns))
		.addPredicate(predRun)
		.addPredicate(predEvent)
	        .build();


        //Iterating over results
	long count=0;
	
	while (scanner.hasMoreRows()) {
        	RowResultIterator results = scanner.nextRows();
	        while (results.hasNext()) {
        	  RowResult result = results.next();
	          	System.out.println((count++)+": "+result.rowToString());
			
        }
	 System.out.println("Took "+(System.currentTimeMillis()-startTime)+" ms");

      }
	
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

