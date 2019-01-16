

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Level;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.glassfish.hk2.utilities.reflection.Logger;


public class partitionHDFS {
static Dataset<Row> fin = null;
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration ();
		   conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
           conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
           conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
           conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
           conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
           conf.set("fs.default.name", "hdfs://192.168.0.184:8020");
           conf.set("fs.defaultFS", "hdfs://192.168.0.184:8020");
           //conf.set("hadoop.job.ugi", "remoteUser");
           conf.set("hadoop.ssl.enabled", "false");
          // readFromHDFS("hdfs://192.168.0.184:8020");
           FileSystem fileSystem = FileSystem.get(conf);
           Class.forName("org.apache.hive.jdbc.HiveDriver");
           	SparkSession spark = SparkSession.builder().master("local").appName("Reading spark files").config("spark.sql.warehouse.dir","hdfs://192.168.0.184:8020/user/hive/warehouse")
    				.config("hive.metastore.uris","thrift://192.168.0.184:9083" ).enableHiveSupport().getOrCreate();
           	Dataset<Row> headerTable = spark.read().format("jdbc") .option("url", "jdbc:hive2://192.168.0.184:10000/exf_test") .option("dbtable", "uptest1").option("driver", "org.apache.hive.jdbc.HiveDriver").load();
           	String[] allCol = headerTable.columns();
           	List<String> allColumn = new ArrayList<String>();
           	for(String s: allCol)
           		allColumn.add(s.split("\\.")[1]);
   
   //        	BufferedReader br = new BufferedReader(new InputStreamReader (fileSystem.open(new Path("hdfs://192.168.0.184:8020/user/cloudera/Individual_profile_schema_100.txt"))));//new FileReader(new File(fileSystem.open(new Path("hdfs://192.168.0.184:8020/user/cloudera/Individual_profile_schema_100.txt")))));
           	BufferedReader br = new BufferedReader(new FileReader(new File("/home/exa1/Documents/Hive_updateDOc/shuffled.csv")));
           	List<Integer> columnIndex =new ArrayList<Integer>();
           	String line1="";
           	while((line1=br.readLine())!=null){
			 String[] colsplit =line1.split(",");
			 if(allColumn.size() != colsplit.length)
				 break; //here i have to write a custom exception
			
			 for(int i=0;i<allColumn.size();i++){
					for(int j=0;j<colsplit.length;j++){
						if(allColumn.get(i).equalsIgnoreCase(colsplit[j])){
							columnIndex.add(j);
						}
								
					}
				 
			 }
			 break;
		 }
           	
           	//For API column Headers
           	//Note Use allColumn and columnIndex
           	/*JSONArray arrtxt = new JSONArray();
           	for(int i=0;i<allColumn.size();i++){

				JSONObject temp1 = new JSONObject();
				temp1.put("name", allColumn.get(i));
				temp1.put("tablename", migvar.migrationTablename);
				arrtxt.add(temp1);
           	}*/
           	
           	//Columns adding it into the file
           	
			/*String path = "hdfs://192.168.0.184:8020/user/cloudera/Test/";
			Dataset<Row> header = spark.sql("select * from exf_test.uptest1").limit(0);
			boolean recursive = false;
			RemoteIterator<LocatedFileStatus> ri = fileSystem.listFiles(new Path(path), recursive);
			while (ri.hasNext()){
				String name =ri.next().getPath().getName(); 
			    if(name.startsWith("_")){
			    	
			    }else{
				String pat = path+name;
			    System.out.println(pat);
			    Dataset<Row> partition = spark.read().format("com.databricks.spark.csv").option("header", "false").load(pat);
			    partition.createOrReplaceTempView("TEMP");
			   // Dataset<Row> temp = spark.sql("SELECT * FROM TEMP");
			 //   header = header.union(temp);
		           Dataset<Row> header = spark.read().format("com.databricks.spark.csv").option("header", "true").load("/home/exa1/Documents/Hive_updateDOc/Individual_profile_schema_100.csv").limit(0);
		           header.show();
			   partition = header.union(partition);
			   partition.show();
			    }
			}*/
           //currently not working on the originial partitions of the encrypted
/*           Dataset<Row> fi = spark.read().format("com.databricks.spark.csv").option("header", "true").load("hdfs://192.168.0.184:8020/user/cloudera/Test/Individual_profile_schema.txt");
           Dataset<Row> sec = spark.read().format("com.databricks.spark.csv").option("header", "true").load("/home/exa1/Documents/Hive_updateDOc/Individual_profile_schema_100.csv").limit(1);
         //  Dataset<Row> header = spark.read().format("com.databricks.spark.csv").option("header", "true").load("/home/exa1/Documents/Hive_updateDOc/Individual_profile_schema_100.csv").limit(0);
           //Dataset<Row> sec = fi.filter("swid!=2");
           fi = header.union(fi);
           
          // Dataset<Row> fina = fi;
          // fina.show();
           fi.foreach((ForeachFunction<Row>) row -> {
        	   int index = row.fieldIndex("swid");
        	   fin = fina.filter("swid!=2");
        	  // fin = fin.union(sec);
        	 //  fin.show();
        	   //fin.write().mode("overwrite").option("header", "false").save("hdfs://192.168.0.184:8020/user/cloudera/Test/Individual_profile_schema.txt");
           });


           List<Row> val = new ArrayList<Row>();
           val = fi.select("swid").collectAsList();
           String primary ="";
           for(Row r: val)
        	   primary = r.toString().substring(1, r.toString().length()-1);*/
          
           //Update code
      /*     Dataset<Row> sec = spark.read().format("com.databricks.spark.csv").option("header", "true").load("/home/exa1/Documents/Hive_updateDOc/Individual_profile_schema_100.csv").filter("swid=2");
           Dataset<Row> third = fi.filter("swid!=2").union(sec);
           third.createOrReplaceTempView("DUMMY");
           third.filter("swid=2").show(2);
           System.out.println(third.count());*/
	}

}
