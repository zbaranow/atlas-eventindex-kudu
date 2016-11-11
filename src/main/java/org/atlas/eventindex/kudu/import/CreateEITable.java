package org.cern.atlas.eventindex.kudu;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;


import java.util.ArrayList;
import java.util.List;

public class CreateEITable {

  private static final String KUDU_MASTER = System.getProperty(
      "kuduMaster", "null");

  private static final String TABLE_NAME = System.getProperty(
      "tableName", "KuduTable");


  public static void main(String[] args) {
    System.out.println("Will try to connect to Kudu master at " + KUDU_MASTER);
    System.out.println("Run with -DkuduMaster=myHost:port to override.");
    System.out.println("Will try to create "+ TABLE_NAME+ ", run with -DtableName=tablename to override.");
    System.out.println("-----------------------------------------------");

//establishing connection
    KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER).build();

//delete an old table if already exists
 try{
                client.deleteTable(TABLE_NAME);
        }
         catch (Exception e) {
        }


    try {
      List<ColumnSchema> columns = new ArrayList(2);

//SPECIFYING TABLE SCHEMA (column and names and types, primary key, encoding ,compression)


columns.add(new ColumnSchema.ColumnSchemaBuilder("runnumber", Type.INT64).key(true).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eventnumber", Type.INT64).key(true).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("project", Type.STRING).key(true).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("streamname", Type.STRING).key(true).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("prodstep", Type.STRING).key(true).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("datatype", Type.STRING).key(true).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("amitag", Type.STRING).key(true).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("lumiblockn", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("bunchid", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eventtime", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eventtimenanosec", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eventweight", Type.FLOAT).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("mcchannelnumber", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("lvl1id", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("issimulation", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("iscalibration", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("istestbeam", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l1trigmask", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l2trigmask", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eftrigmask", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("smk", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("hltpsk", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l1psk", Type.INT64).encoding(ColumnSchema.Encoding.BIT_SHUFFLE).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("nam0", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("db0", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("cnt0", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("clid0", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("tech0", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("oid0", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("nam1", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("db1", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("cnt1", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("clid1", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("tech1", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("oid1", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("nam2", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("db2", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("cnt2", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("clid2", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("tech2", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("oid2", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("nam3", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("db3", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("cnt3", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("clid3", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("tech3", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("oid3", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l1trigchainstav", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l1trigchainstap", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l1trigchainstbp", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l2trigchainsph", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l2trigchainspt", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("l2trigchainsrs", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eftrigchainsph", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eftrigchainspt", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
columns.add(new ColumnSchema.ColumnSchemaBuilder("eftrigchainsrs", Type.STRING).encoding(ColumnSchema.Encoding.DICT_ENCODING).nullable(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());

      Schema schema = new Schema(columns);

//SETTING PARTITION SCHEMA


//setting partitioning columns
List<String> partColumns = new ArrayList<>();
partColumns.add("runnumber");
//partColumns.add("project");
//partColumns.add("streamname");
//partColumns.add("prodstep");
//partColumns.add("datatype");
//partColumns.add("amitag");
//CreateTableOptions options = new CreateTableOptions().addHashPartitions(partColumns, 12).setNumReplicas(2);




CreateTableOptions options = new CreateTableOptions();

options.setRangePartitionColumns(partColumns);
 PartialRow row = null;


//Create range partition for each 250k runnumber ids

for (int i =175000; i<=400000;i+=25000)
{
	row= new PartialRow(schema);
	row.addLong("runnumber",i);
	options.addSplitRow(row);
}
	
//creating subbackets

        options.addHashPartitions(partColumns, 4);
	options.setNumReplicas(3);



//CREATE TABLE

      client.createTable(TABLE_NAME, schema,options);


    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
          client.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      
    }
  }
}

