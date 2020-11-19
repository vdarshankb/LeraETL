package org.lera

import java.io.FileInputStream
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.sql.{Connection, DriverManager, Statement}

import org.lera.etl.util.utils.{OptionUtils, PropertyException, configDatabase, configTable}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, RuntimeConfig, SparkSession}
import org.lera.connectionContextCreator.getProperty
import org.lera.etl.util.Constants.{environment, loggerLevel}
import org.lera.etl.util.jdbcConnector.{connection, executeQueryUsingImpala}

import scala.util.{Failure, Success}
import scala.util.Try
import org.lera.connectionContextCreator._
import org.lera.etl.util.jdbcConnector

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

  private val logger: Logger = Logger.getLogger(connectionContextCreator.getClass)
  private var spark: SparkSession = _

  def close() = {
    logger.info("Closing both connection object and spark session")
    connection.close()
    spark.close()
  }

  /*
   * get property values from spark conf instance
   * @param key key for getting the value
   * @return
   * */
  def getProperty(key: String): String ={
    logger.info(s"Get the property for the key: $key")
    getConf.getOption(key).getNonEmptyOrElse(throw PropertyException(key))
  }

  /* returns spark conf instance
  *
  * @return
  */
  def getConf: RuntimeConfig = {
    logger.info("Getting the run time spark configuraaton ")
    getSparkSession.conf
  }

  /* Get the spark session context
  *
  * @return
   */
  def getSparkSession: SparkSession = {
    logger.info("Inside the getSparkSession method in the package.scala and session state is as below:")
    logger.info(null == spark)
    if (null == spark) {
      logger.info("Creating the Spark Session")
      connectionContextCreator.apply()
    }
    spark
  }

  def apply(): SparkSession = {

    logger.info("Inside the connectionContextCreator.apply method.")

    //app name should be passed from the priperties file as (spark.app.name)
  //  if (null == spark) spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    import scala.collection.JavaConverters._
    var propertiesFilePath: String = ""

    //Below lines of code is required for testing in local machine only
    if (environment == "Lera")
      {
        if (null == spark) spark = SparkSession.builder().enableHiveSupport().getOrCreate()
        propertiesFilePath = "/home/spark/lera_etl/spark_lera_conf.properties"
      }
    else {
      System.setProperty("hadoop.home.dir", "C:\\winutils")
      spark = SparkSession
       .builder()
       .master("local")
       .config("spark.sql.warehouse.dir", "C:\\temp\\SparkWarehouse\\")
       .getOrCreate()
      propertiesFilePath = "c:\\temp\\spark_lera_conf.properties"
    }

    val property = new Properties()
    property.load(new FileInputStream(propertiesFilePath))

    val properties: mutable.Map[String, String] = property.asScala

    for (key <- properties) {
      spark.conf.set(key._1, key._2)
    }

    val logLevel: String = spark.conf.getOption(loggerLevel).orNull
    Logger.getRootLogger.setLevel(Level.toLevel(logLevel, Level.INFO))

    spark
  }

    /* Spark Session can be assigned or modified if it is created outside of the context creator
    *
    *@param spark spark session instance
     */

    def setSparkSession(spark: SparkSession) = {
      this.spark = spark
    }

  /*
 *Executes query using spark sql
 *
 *@param queries prepared query statement
 **/

  def executeHiveQuery(queries: String*): Boolean = {
    logger.info(s"Executing queries: $queries")
    Try {
      queries.foreach(query => {
        logger.info(s"Executing query $query")
        getSparkSession.sql(query)
      })
    } match {
      case Success(_) => true
      case Failure(exception) =>
        logger.error(s"Query execution failed due to ${exception.getMessage}")
        throw exception
    }
  }

  def createTables(): Unit =
  {
    spark.sql("CREATE DATABASE IF NOT EXISTS default")
    spark.sql("CREATE TABLE IF NOT EXISTS default.audit( source_system string, sourcedata_regionname string, target_table string,load_type string, start_time timestamp,update_time timestamp, runState string, errormessage string ) row format delimited fields terminated by ','")
    spark.sql("CREATE TABLE IF NOT EXISTS default.generic_config(source_system string,sourcedata_regionname string, property_name string,property_value string, table_order int) row format delimited fields terminated by ','")
    spark.sql("CREATE TABLE IF NOT EXISTS default.default_values( source_system string, sourcedata_regionname string, target_table string, target_column string, default_value string ) row format delimited fields terminated by ',' ")
    spark.sql("CREATE TABLE IF NOT EXISTS default.column_mapping(source_system string,sourcedata_regionname string, source_table string, target_table string,source_column string, target_column string ) row format delimited fields terminated by ',' ")
    spark.sql("CREATE TABLE IF NOT EXISTS default.filter_data( source_system string, sourcedata_regionname string, source_table string, filter_col_name string, filter_condition string, filter_value string, logical_operator string, filter_order int, group_order int ) row format delimited fields terminated by ','")
    spark.sql("CREATE TABLE IF NOT EXISTS default.delete_data( source_system string, sourcedata_regionname string, source_table string, delete_col_name string, delete_condition string, delete_value string, logical_operator string, delete_order int, group_order int ) row format delimited fields terminated by ',' ")
    spark.sql("CREATE TABLE IF NOT EXISTS default.join_table_mapping( source_system string, sourcedata_regionname string, source_database string, source_table string, target_database string, target_table string, lookup_database string, lookup_table string, lookup_table_type string, source_column string, lookup_column string, join_type string, join_order int ) row format delimited fields terminated by ',' ")
    spark.sql("CREATE TABLE IF NOT EXISTS default.partition_column(source_system string, sourcedata_regionname string,database_name string, table_name string, partition_column string, partition_column_type string, number_of_partitions int, lower_bound int, upper_bound int ) row format delimited fields terminated by ',' ")
    spark.sql("CREATE TABLE IF NOT EXISTS default.generic_lookup( source_system string, sourcedata_regionname string, source_database string, source_table string, target_database string, target_table string, lookup_database string, lookup_table string, lookup_table_type string, source_column string, lookup_column string, lookup_order int ) row format delimited fields terminated by ',' ")
    spark.sql("CREATE TABLE IF NOT EXISTS default.orders( OrderDate string, Region string, Rep string, Item string, Units decimal(10,6), UnitCost decimal(10,6), Total decimal(10,6) ) row format delimited fields terminated by ',' ")
    spark.sql("CREATE TABLE IF NOT EXISTS default.orders_stg( OrderDate string, Region string, Rep string, Item string, Units decimal(10,6), UnitCost decimal(10,6), Total decimal(10,6) ) row format delimited fields terminated by ',' ")

  }

}