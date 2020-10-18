package org.lera

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.lera.etl.readers.{KuduReader, Reader}

package object etl extends ContextCreator {

  val getReaderInstance:String => Reader = {
    case _ => KuduReader
  }
  val ibpAuditDatabase = getProperty("spark.audit_database")
  val ibpConfigDatabase = getProperty("spark.audit_database")

  def getTableConfig(sourceSystem: String,
                     region: String,
                     loadType: String,
                     tableNames: String) = {
    TableConfig(
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
                       file_path: String,
                       message: String = "")
case class PartitionTableConfig(tableName:String)
trait ContextCreator {

  lazy val spark = SparkSession.builder().appName("test").getOrCreate()
  lazy val getConf = spark.conf

  def getProperty(propertyName: String): String = {
    spark.conf.get(propertyName)

  }

}
