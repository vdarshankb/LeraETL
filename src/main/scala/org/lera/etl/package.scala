package org.lera

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.sql.{Connection, DriverManager, Statement}

import org.lera.etl.util.utils.JDBC_URL_Generator
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, RuntimeConfig, SparkSession}
import org.lera.ContextCreator.getProperty
import org.lera.etl.readers.{KuduReader, Reader}
import org.lera.etl.util.Constants.fullLoadType
import org.lera.etl.util.ImpalaConnector
import org.lera.etl.util.Parser.logger

package object etl {

  val etlAuditDatabase: String = getProperty("spark.audit_database")
  val etlConfigDatabase: String = getProperty("spark.config_database")

  def getTableConfig(sourceSystem: String,
                     region: String,
                     loadType: String,
                     tableNames: String): TableConfig = {
    TableConfig(
      sourceSystem,
      region,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    )
  }

  implicit class DataFrameImplicits(df: DataFrame) {

    def trimColumnTrailingSpace(): DataFrame = {
      df.columns.foldLeft(df)((df, column) => {
        df.withColumn(column, trim(col(column)))
      })
    }
  }
}

case class TableConfig(   source_system: String,
                          sourcedata_regionname: String,
                          source_table_type: String,
                          target_table_type: String,
                          source_database: String,
                          source_table: String,
                          file_path: String,
                          target_database: String,
                          target_table: String,
                          load_type: String,
                          source_increment_column: String,
                          target_increment_column: String,
                          message: String = ""
                        ) extends Properties

case class PartitionTableConfig(tableName: String)

case class DateTimeConvert(
                            sourceColumnName:String,
                            targetColumnName:String,
                            sourceFormat:String,
                            targetFormat:String
                          )
case class TimeConvert(sourceColumn: String,
                       targetColumn: String,
                       sourceTime: TimeUnit,
                       targetTime: TimeUnit)



//trait ContextCreator {

object ContextCreator {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("etlapplication")
    .enableHiveSupport()
    .getOrCreate()


  //Below are the setup for connecting to lera cluster
  val jdbcURLString: String = "jdbc:hive2://10.22.1.66:2181,10.22.1.66:2181:2181,10.22.1.67:2181/default;password=welcome;serviceDiscoveryMode=zooKeeper;user=spark;zooKeeperNamespace=hiveserver2"
  spark.conf.set("hive.metastore.uris", jdbcURLString)

  //Below are the setup for connecting to local laptop cluster
 // val jdbcURLString: String = "jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default;password=maria_dev;serviceDiscoveryMode=zooKeeper;user=maria_dev;zooKeeperNamespace=hiveserver2"

  /*lazy val getConf: RuntimeConfig = spark.conf
  lazy val sparkConf: RuntimeConfig = spark.conf
*/

  val session: SparkSession = spark

/*
  spark.conf.set("spark.hive.mapred.supports.subdirectories","true")
  spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
*/

//  spark.sql("SELECT * from default.generic_config").show(2)

  println("Reading properties file..")

  val property = new Properties()
  property.load(new FileInputStream("/home/spark/lera_etl/spark_lera_conf.properties"))

  import scala.collection.mutable
  import scala.collection.JavaConverters._

  val properties: mutable.Map[String, String] = property.asScala

  for (key <- properties){
    spark.conf.set(key._1, key._2)
  }
/*
  println("executed the sql query using spark session")
  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val df = spark.read.format("jdbc").option("url", jdbcURLString)
    .option("etl_config", "generic_config").load()
  df.show()

  println("executed the sql query using jdbc driver")
*/

  def getProperty(propertyName: String): String = {
    println(s"Fetching the property value: $propertyName")
    spark.conf.get(propertyName)
  }


}
