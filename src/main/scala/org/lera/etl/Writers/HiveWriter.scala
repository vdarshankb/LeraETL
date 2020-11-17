package org.lera.etl.Writers

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.lera.TableConfig
import org.lera.etl.util.Constants

import scala.util.{Success, Try}
import org.lera.etl.util.utils._
import org.lera.etl.util.Constants._
import org.lera.etl.util.Enums.RunStatus._

import scala.collection.parallel.ParSeq
import org.lera.connectionContextCreator.getSparkSession
import org.lera.etl.util.jdbcConnector
import org.lera.etl.util.jdbcConnector.executeQuery

/*
import org.lera.ContextCreator.spark
import org.lera.ContextCreator.session
import org.lera.etl.util.ImpalaConnector._
*/


object HiveWriter extends Writer {

  private val logger: Logger = Logger.getLogger(HiveWriter.getClass)
  logger.info("Inside the Hive Writer object")

  /*
   * Write datasets into target table or file system
   * @param dataSet tuple of data set and table properties
   *
   * */

  override def write(dataSet: ParSeq[(TableConfig, DataFrame)]): Unit = {

    dataSet.foreach(tuple => {

      val (tableConf: TableConfig, df: DataFrame) = tuple
      val tableName: String = tableConf.target_table
      val load_type: String = tableConf.load_type

      handler(tuple._1) {

        val finalTargetTableName = s""
        logger.debug(
          s"Load Type for the table $finalTargetTableName is $load_type"
        )

        logger.info(s"Loading data into target table $finalTargetTableName")

        val partitionColumns: String = readPartitionColumns(finalTargetTableName)
        val updatedDf: DataFrame =
          if (load_type.equalsIgnoreCase(Constants.fullLoadType) || load_type
                .toLowerCase()
                .contains(Constants.fullLoadType.toLowerCase)) {
            validateFullLoad(df, tableConf.source_system, finalTargetTableName)
          } else df

        updatedDf.createTempView(viewName = s"${tableName}_temp_view")
        val columns = updatedDf.columns.mkString(StringExpr.comma)

        if (partitionColumns.isEmpty) {
          loadNonPartitionedData(tableName, finalTargetTableName, columns)
        } else {
          loadPartitionedData(
            tableName,
            finalTargetTableName,
            partitionColumns,
            columns
          )
        }

      }

      val finalTargetTableName=s"${tableConf.target_database}.${tableConf.target_table}"

      validateTableMetadata(finalTargetTableName)

      logger.info(
        s"Data loaded successfully into target Hive table $finalTargetTableName"
      )

      auditUpdate(tableConf, SUCCESS)

    })
  }

  def loadNonPartitionedData(intermediateTable: String,
                             targetTableName: String,
                             columns: String): Unit = {

    logger.info(s"Inserting data into non partitioned table $targetTableName")
    logger.debug(
      s"Executing query : INSERT INTO TABLE $targetTableName SELECT $columns FROM $intermediateTable"
    )
    getSparkSession.sql(s"INSERT INTO TABLE $targetTableName SELECT $columns FROM $intermediateTable"
    )

  }

  def validateTableMetadata(targetTableName: String)(): Unit = {

    Try {
      executeQuery("SET QUERY_TIMEOUT_S-120;")
      executeQuery(s"INVALIDATE METADATA $targetTableName")
    } match {
      case Success(_) =>
        logger.info(s"Invalidated metadata for table $targetTableName")
      case _ =>
        logger.warn(s"Failed to invalidate metadata for $targetTableName")
    }
  }

  private def loadPartitionedData(intermediateTable: String,
                                  targetTableName: String,
                                  partitionColumns: String,
                                  columns: String): Unit = {

    logger.info(s"Inserting data into partitioned table $targetTableName")
    logger.info(
      s"Partition columns for table $targetTableName are $partitionColumns"
    )

    getSparkSession.sql("SET hive.exec.dynamic.partition = true")
    getSparkSession.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
    logger.debug(
      s"Executing query : INSERT OVERWRITE TABLE $targetTableName PARTITION $partitionColumns"
    )
    getSparkSession.sql(s"INSERT OVERWRITE TABLE $targetTableName PARTITION $partitionColumns"
    )

  }

  private def validateFullLoad(df: DataFrame,
                               source_system: String,
                               targetTableName: String): DataFrame = {
    import org.apache.spark.sql.functions._
    val fullLoadDf = if (isSourceBasedLoad(source_system)) {

      val cond: DataFrame => DataFrame = targetDataFrame =>
        if (df.rdd.isEmpty()) {
          targetDataFrame
        } else {
          val sourceSystemNames: Array[String] =
            getSourceSystemNames(df)

          targetDataFrame.where(
            !lower(trim(col(sourceSystem)))
              .isin(sourceSystemNames: _*)
          )
      }

      val unionDf: DataFrame = df.union(cond(readHiveTable(targetTableName)))
      unionDf.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName = s"${targetTableName}_temp")
      readHiveTable(tableName = s"${targetTableName}_temp")
    } else df
    getSparkSession.sql(s"TRUNCATE TABLE $targetTableName")
    fullLoadDf
  }

}
