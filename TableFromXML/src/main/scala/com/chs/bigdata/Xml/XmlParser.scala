package com.chs.bigdata.Xml
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
import org.apache.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import com.databricks.spark.xml._
import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager, Logger}
import java.net.URI;
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.util.Properties



object HDFSFileService{
  private val conf = new Configuration()
  private val fileSystem = FileSystem.get(conf)

  def getFilesWithExtension(folderPath: String, extension: String): List[(String, String)] = {
    val path = new Path(folderPath)
    if (fileSystem.exists(path) && fileSystem.isDirectory(path)) {
    
    val files = fileSystem.listStatus(path)
    files.filter(f => f.isFile() && f.getPath.toString().toLowerCase().endsWith(s".${extension}")).map{
      case f => (f.getPath.toString(),f.getPath.getName.toLowerCase.replaceAll(s".${extension}", ""))
    }.toList
    
  }else{
    List.empty
  }
  }
  
}


object XmlParse {
  def main(args: Array[String]): Unit = {
	Logger.getLogger("org").setLevel(Level.OFF)
	Logger.getLogger("akka").setLevel(Level.OFF)
	val logger = Logger.getLogger("XmlParse")
      if (args.length < 2) {
        System.err.println("Usage: XmlParse accepts two arguments <path_to_config_file> <droptable>")
        System.err.println(args.length)
        System.exit(1)
      }
	val pathToConfile:String = args(0)
	val dropTable:String = args(1)
	logger.info("Configuration file is " +pathToConfile+ "")
	logger.info("This argument has to be  droptable if you want to drop existing table " +dropTable+ "")
	val conf = new SparkConf().setAppName("CreateTableFromXML")
	val sc = new SparkContext(conf)
	val sqlContext = new SQLContext(sc)
	val hivcontext = new HiveContext(sc)
	import hivcontext.implicits._
	import hivcontext.sql
  
    
  
    val myCfg =  ConfigFactory.parseFile(new File(pathToConfile))
    val appConf = myCfg.getConfig("job-config")
    val inputPath =  appConf.getString("input-path")
    val outputPath =  appConf.getString("output-path")
    val paramts = appConf.getConfig("parameters")
    val delimiter = paramts.getString("delimiter")
    val databases = paramts.getString("database")
    val databaselist = databases.split(",").toList
    val extension = paramts.getString("extension")
    logger.info("Done with assigning parameters to variables input base path is " +inputPath+ ", output base path is " +outputPath+ ", delimiter is " +delimiter+ ", database list is " +databaselist+ ", extension of file is " +extension+ "")
	
	
 
    try{ 
	val filelists = HDFSFileService.getFilesWithExtension(inputPath,extension)
    logger.info("Done with creating a list of path to XML files, start looping through the list of databases and paths " + filelists + "")
    for (database <- databaselist) {
    println(database)
    logger.info("All is set to read XML file, generate DDL and start creating target database and tables for database " + database + "")
    filelists.foreach{
    case getpathtable=>
    val df = sqlContext.read
    .format("com.databricks.spark.xml")
    .option("rowTag", "field")
    .load(getpathtable._1)
    
    val queryBuilderdb: StringBuilder = new StringBuilder
    queryBuilderdb.append("CREATE DATABASE IF NOT EXISTS ")
    queryBuilderdb.append(database)
    queryBuilderdb.append(" LOCATION '" + outputPath + database + "'")
    println(queryBuilderdb.toString)
	
    val xmrdd = df.select("field_name", "field_type").rdd.collect
    var countclm = xmrdd.length
    val queryBuilder: StringBuilder = new StringBuilder
    queryBuilder.append("CREATE TABLE IF NOT EXISTS `")
    queryBuilder.append(database)
    queryBuilder.append("`.`")
    queryBuilder.append(getpathtable._2)
    queryBuilder.append("`(")
    queryBuilder.append(" `FACILITY_ID` string,")
    queryBuilder.append(" `UPDATE_DATE` string,")
    xmrdd.foreach{
      case tplefield=>
        if (tplefield(1)=="signed numeric"){
          println(tplefield(0))
          queryBuilder.append("`")
          queryBuilder.append(tplefield(0).toString().replaceAll("[^a-zA-Z0-9]", ""))
          countclm-=1
          queryBuilder.append("` ")
          queryBuilder.append("string")
          if(countclm!=0){
          queryBuilder.append(", ")
          }
        }else if (tplefield(1)=="character"){
          println(tplefield(0))
          queryBuilder.append("`")
          queryBuilder.append(tplefield(0).toString().replaceAll("[^a-zA-Z0-9]", ""))
          countclm-=1
          queryBuilder.append("` ")
          queryBuilder.append("string")
          if(countclm!=0){
          queryBuilder.append(", ")
          }
        }else if (tplefield(1)=="time"){
          println(tplefield(0))
          queryBuilder.append("`")
          queryBuilder.append(tplefield(0).toString().replaceAll("[^a-zA-Z0-9]", ""))
          countclm-=1
          queryBuilder.append("` ")
          queryBuilder.append("timestamp")
          if(countclm!=0){
          queryBuilder.append(", ")
          }
        }else if (tplefield(1)=="date field"){
          println(tplefield(0))
          queryBuilder.append("`")
          queryBuilder.append(tplefield(0).toString().replaceAll("[^a-zA-Z0-9]", ""))
          countclm-=1
          queryBuilder.append("` ")
          queryBuilder.append("date")
          if(countclm!=0){
          queryBuilder.append(", ")
          }
        }else{
          println(tplefield(0))
          queryBuilder.append("`")
          queryBuilder.append(tplefield(0).toString().replaceAll("[^a-zA-Z0-9]", ""))
          countclm-=1
          queryBuilder.append("` ")
          queryBuilder.append("string")
          if(countclm!=0){
          queryBuilder.append(", ")
          }
        }

  }
          queryBuilder.append(", `HDP_REC_CHG_IND` string")
          queryBuilder.append(") " +
          "ROW FORMAT DELIMITED " +
          "FIELDS TERMINATED BY '" + delimiter + "' " +
          "STORED AS TEXTFILE" + " Location " + "'" + outputPath + database + "/" + getpathtable._2 +  "'")
	println(queryBuilder.toString())
	println(countclm)
	logger.info("DDL generation for " + getpathtable._2 + " has completed")
	val database_query = queryBuilderdb.toString()
	val table_sql_query = queryBuilder.toString()
	val drop_table = "DROP TABLE IF EXISTS `" + database + "`.`" + getpathtable._2 + "`"
	println(drop_table)
  
	try{
	if (dropTable=="droptable")
	{
    hivcontext.sql(drop_table)
	logger.warn("Table " +drop_table+ " droped if it exists")
	hivcontext.sql(database_query)
    logger.info("Database " +database_query+ " created if it does not exist")
    hivcontext.sql(table_sql_query)
    logger.info("Table " +table_sql_query+ " created")
	}else
	{
	hivcontext.sql(database_query)
	logger.warn("Database " +database_query+ " created if it does not exists")
    hivcontext.sql(table_sql_query)
	logger.info("Table " +table_sql_query+ " created if it does not exists")
	}
  }catch{
    case e: Exception => println("Failed with exception " +e+ "")
    logger.error("One of the following query failed " + drop_table + " ||| " +table_sql_query+ " ||| " +database_query+ "")
    //println("could not drop or create table " + drop_table + "|||" + table_sql_query + "")
  }
    
    
    
}
}
  } catch{
      case e: Exception => println(e)
      logger.error("Could not parse XML file")
      //println("could not pass XML file")
   }
    
    
}
}