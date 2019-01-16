import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Spark2Hive {
// used spark-hive-2.11
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	/*	Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkSession spark = SparkSession.builder().master("local")
				.appName("Reading spark files").
				//config("hive.metastore.uris","thrift://192.168.0.184:9083").enableHiveSupport().
				getOrCreate();*/
		//config("hive.metastore.warehouse.dir", "hdfs://192.168.0.184:8020/user/hive/warehouse/exf_test.db").
/*		Dataset<Row> encryptcol = spark.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load("/home/exa1/Desktop/testout.txt").na().fill("");
		SparkContext context = spark.sparkContext();
	    context.setLogLevel("ERROR");

	    SQLContext sqlCtx = spark.sqlContext();
	    
	    encryptcol.write().mode("append").
	    //save("hdfs://192.168.0.184:50070/user/hive/warehouse/exf_test.db/test1");
	    saveAsTable("emp");*/
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkSession spark = SparkSession.builder().master("local").appName("Reading spark files")
				.config("spark.sql.warehouse.dir","hdfs://192.168.0.184:8020/user/hive/warehouse")
				.config("hive.metastore.uris", "thrift://192.168.0.184:9083")
				.enableHiveSupport().getOrCreate();
	//	SQLContext sqlcontext = new org.apache.spark.sql.hive.HiveContext(spark);
		Dataset<Row> encryptcol = spark.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load("/home/exa1/Desktop/testout.txt").na().fill("");
		encryptcol.write().format("orc").mode("append").save("hdfs://192.168.0.184:8020/user/cloudera/orc1");
			/*		//encryptcol.show();
		Properties prop = new Properties();
prop.put("user", ""); prop.put("password", "");
		//prop.put("user", "secured");
	//	prop.put("password", "pwd4secured");
		//Dataset<Row> plaincol = spark.read().jdbc("jdbc:mysql://targetschemas.cqzcaawwqsmw.us-east-1.rds.amazonaws.com:3306/individual_src", "individual_profile_schema", prop);//.limit(10);
		Dataset<Row> plaincol = spark.read().jdbc("jdbc:hive2://192.168.0.184:10000/individual_profile_schema_src", "individual_profile_schema_src", prop);//.limit(10);
		plaincol.createOrReplaceTempView("SBTEST1");
		
		System.out.println(plaincol.count());*/
/*		Dataset<Row> firstset = spark.sql("SELECT master_individual_id,swid,customer_id,marketing_effort_key,template_key,title,middle_initial,geography_key,age,facebook_connect_flg,employee_flg,registration_date,registration_affiliate_name,marketing_effort_code_key,registration_flg,affluence,affluent_suburbia,numzip,tenure,postal_code,country,state_province,state_abbreviation,city,dma_code,dma_name,msa_name,age_range,ethnic_code,ethnic_group,ethnic_group_cd,household_income,aerobic_exercise_ind,assimilation_cd,cable_tv_ind,child_age_0to2_present,child_age_3to5_present,child_age_6to10_present,child_age_11to15_present,child_age_16to17_present,card_holder,gas_department_retail_card_holder,travel_entertainment_card_holder,premium_card_holder,dwelling_type_cd,first_individual_age_range_cd,first_individual_education_cd,first_individual_gender_cd,first_individual_occ_cd,home_assessed_value_num,home_equity_available_cd,home_market_value_cd,home_owner_ind,own_rent,household_size_cd,international_travel_ind,investing_grouping_ind,length_of_residence_cd,likely_investors_ind,marital_status,net_worth_cd,number_of_adults,number_of_children,personal_investments_ind,children_present,running_exercise_ind,satellite_dish_tv_ind,second_individual_age_range_cd,second_individual_education_cd,second_individual_gender_cd,stocks_bonds_investment_ind,vacation_travel_rv_ind"
 + " FROM individual_profile_schema_src.individual_profile_schema_src");
		firstset.show();*/
	/*	Dataset<Row> listrows = spark.sql("show databases");
		listrows.show();*/
	}

}
