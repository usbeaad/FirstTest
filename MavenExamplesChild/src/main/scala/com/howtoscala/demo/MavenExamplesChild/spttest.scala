package com.howtoscala.demo.MavenExamplesChild
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._
import scala.xml._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType};
import java.io.File
import com.typesafe.config.ConfigFactory

object spttest extends App{
  val conf = new SparkConf().setMaster("local[*]").setAppName("mergerfiles")
    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //val myConfigFile = new File("C:\\Users\\aborode\\Desktop\\DevFolder\\TESTFOLDERDELETE\\")
  //val fileConfig = ConfigFactory.parseFile(myConfigFile).getConfig("myconfig")
  //val config = ConfigFactory.load(fileConfig)
  val testparrl = sc.parallelize(List("/repository/resources/2016-03-04/file.csv"))
  
   def spltt(ffile:String): String= {
        val b = ffile.split("/").mkString("/")
        b
      }
  
   val splout = testparrl.map(x=>spltt(x))
   splout.foreach(println)
   
  val myCfg =  ConfigFactory.parseFile(new File("C:\\Users\\aborode\\Desktop\\DevFolder\\TESTFOLDERDELETE\\myconfig.conf"))
  val appConf = myCfg.getConfig("job-scheduler")
  val configPath = appConf.getString("config-path")
  val configOut =  appConf.getString("config-output")
  println(configPath)
  println(configOut)
   def getRecursiveListOfFiles(dir: String): Array[String] = {
    (new File(dir))
        .listFiles
        .filter(_.isDirectory)
        .map(_.getPath)
      }
  
  def getdirname(directoryName: String): Array[String] = {
    (new File(directoryName))
        .listFiles
        .filter(_.isDirectory)
        .map(_.getName)
}
      def mrgfiles(ffile:String): String= {
        val b = ffile.split("~").mkString("~")
        b
      }
    //val basedir = "C:\\Users\\aborode\\Desktop\\DevFolder\\TESTFOLDERDELETE\\"
    val outpt = getRecursiveListOfFiles(configPath).toList
    outpt.foreach{
      case config =>
          println(config)
        	val file = sc.textFile(config)
        	val r = scala.util.Random
        	val rnum = r.nextInt(1000)
        	//println(r.nextInt(1000))
        	
        	val outf = file.map(x=>mrgfiles(x))
        	outf.coalesce(1).saveAsTextFile(configOut + rnum + "_output")
    }
}