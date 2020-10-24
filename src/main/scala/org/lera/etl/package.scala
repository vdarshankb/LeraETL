package org.lera

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, RuntimeConfig, SparkSession}
import org.lera.etl.readers.{KuduReader, Reader}
import org.lera.etl.util.Constants.fullLoadType
package object etl extends ContextCreator {


  val ibpAuditDatabase: String = getProperty("spark.audit_database")
  val ibpConfigDatabase: String = getProperty("spark.audit_database")

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

case class TableConfig(source_table: String,
                       source_database: String,
                       target_table: String,
                       target_database: String,
                       load_type: String,
                       incremental_column: String,
                       source_system: String,
                       sourcedata_regionname: String,
                       source_table_type: String,
                       target_table_type: String,
                       target_increment_column: String,
                       source_increment_column: String,
                       file_path: String,
                       message: String = "")
    extends Properties
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

trait ContextCreator {

  lazy val spark: SparkSession =
    SparkSession.builder().appName("test").getOrCreate()
  lazy val getConf: RuntimeConfig = spark.conf
  lazy val sparkConf: RuntimeConfig = spark.conf
  val session: SparkSession = spark

  def getProperty(propertyName: String): String = {
    spark.conf.get(propertyName)

  }

}
