package org.lera

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.TimeUnit

import java.sql.{Connection, DriverManager, Statement}

import org.lera.etl.util.utils.{OptionUtils, PropertyException}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, RuntimeConfig, SparkSession}
import org.lera.connectionContextCreator.{getProperty}
import org.lera.etl.util.Constants.{ loggerLevel}
import org.lera.etl.util.jdbcConnector.connection
import org.lera.connectionContextCreator._

import scala.collection.mutable

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
object connectionContextCreator
{

  private var spark: SparkSession = _

  def close() = {
    org.lera.etl.util.jdbcConnector
    connection.close()
    spark.close()
  }

  /*
   * get property values from spak conf instance
   * @param key key for getting the value
   * @return
   * */
  def getProperty(key: String): String ={
    getConf.getOption(key).getNonEmptyOrElse(throw PropertyException(key))
  }

  /* returns spark conf instance
  *
  * @return
  */
  def getConf: RuntimeConfig = {
    getSparkSession.conf
  }

  /* Get the spark session context
  *
  * @return
   */
  def getSparkSession: SparkSession = {
    if (null == spark)
      connectionContextCreator.apply()
    spark
  }

  def apply(): SparkSession = {
    //app name should be passed from the priperties file as (spark.app.name)
    spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import scala.collection.JavaConverters._

    //Below lines of code is required for testing in local machine only
    /*
     spark = SparkSession.builder().master("local").getOrCreate()
 */

    val propertiesFilePath: String = "/home/spark/lera_etl/spark_lera_conf.properties"
    println("Reading properties file..")

    val property = new Properties()
    property.load(new FileInputStream(propertiesFilePath))

    val properties: mutable.Map[String, String] = property.asScala

    for (key <- properties) {
      spark.conf.set(key._1, key._2)
    }

    val logLevel: String = spark.conf.getOption(loggerLevel).orNull
    Logger.getRootLogger.setLevel(Level.toLevel(logLevel, Level.DEBUG))

    spark
  }

    /* Spark Session can be assigned or modified if it is created outside of the context creator
    *
    *@param spark spark session instance
     */

    def setSparkSession(spark: SparkSession) = {
      this.spark = spark
    }

}

/*
object ContextCreator {

 val spark: SparkSession = SparkSession
   .builder()
   .master("local")
   .appName("etlapplication")
   .enableHiveSupport()
   .getOrCreate()


//Below are the setup for connecting to lera cluster
val jdbcURLString: String = "jdbc:hive2://10.22.1.66:2181,10.22.1.66:2181:2181,10.22.1.67:2181/default;password=welcome;serviceDiscoveryMode=zooKeeper;user=spark;zooKeeperNamespace=hiveserver2"
val propertiesFilePath = "/home/spark/lera_etl/spark_lera_conf.properties"

//Below are the setup for connecting to local laptop cluster
// val jdbcURLString: String = "jdbc:hive2://sandbox-hdp.hortonworks.com:2181/default;password=maria_dev;serviceDiscoveryMode=zooKeeper;user=maria_dev;zooKeeperNamespace=hiveserver2"
// val propertiesFilePath = "/home/maria_dev/spark_lera_conf.properties"

spark.conf.set("hive.metastore.uris", jdbcURLString)

lazy val getConf: RuntimeConfig = spark.conf
lazy val sparkConf: RuntimeConfig = spark.conf

val session: SparkSession = spark

println("Reading properties file..")

val property = new Properties()
property.load(new FileInputStream(propertiesFilePath))

import scala.collection.mutable
import scala.collection.JavaConverters._

val properties: mutable.Map[String, String] = property.asScala

for (key <- properties){
 spark.conf.set(key._1, key._2)
}


println("Displaying the contents of the default.generic_config table")
spark.sql("SELECT * from default.generic_config").show(10)

def getProperty(propertyName: String): String = {
 println(s"Fetching the property value: $propertyName")
 spark.conf.get(propertyName)
}



}
*/