package org.lera.etl.readers

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import java.util.Properties
import scala.io.Source
import org.apache.spark.SparkConf

trait PipelineBase {
  
  val url = getClass.getResource("application.properties")
	  val properties: Properties = new Properties()
    
    if (url != null) {
    val source = Source.fromURL(url)
    properties.load(source.bufferedReader())
    }
    
    /* Creating Spark session object*/
    val appname = properties.getProperty("app_name")
    val mastr = properties.getProperty("master")
  
  implicit val spark = SparkSession
    .builder()
    .appName(appname)
    .master(mastr)
    .getOrCreate()
    
}