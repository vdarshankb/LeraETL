package org.lera.etl.readers

import org.apache.log4j.Logger
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row}
import org.lera.TableConfig
import org.lera.etl.util.Constants
import org.lera.etl.util.Constants._
import org.lera.etl.util.utils._
import org.lera.etl.util.Enums.Writers.writerType

object KuduReader extends Reader {

  private val logger: Logger = Logger.getLogger(KuduReader.getClass)

  /*
   * Read data from kudu tables as full read or partial read(incremental)
   *
   *@param tableConf source and target table details
   *@return
   */

  override def readData(tableConf: TableConfig): (TableConfig, DataFrame) = {

    logger.info(
      s"Reading data from Kudu table ${tableConf.source_database}.${tableConf.source_table}"
    )

    val dataFrame: DataFrame = tableConf.load_type.toLowerCase match {

      case Constants.incremental | Constants.incr =>
        val incrementalCondition: String = getIncrementalLoadFilterCondition(
          tableConf
        )

        logger.info(s"Incremental condition is $incrementalCondition")
        readKuduTableWithTableConfig(tableConf, incrementalCondition)

      case Constants.fullLoadType =>
        readKuduTableWithTableConfig(tableConf)
    }

    (tableConf, dataFrame)
  }

  private def getIncrementalLoadFilterCondition(
    tableConf: TableConfig
  ): String = {

    val incDF: DataFrame = getIncrementalColumnDf(tableConf)

    val maxValueArr: Array[Row] =
      incDF.filter(_.getAs(0) != null).collect

    if (maxValueArr.isEmpty) {
      StringExpr.empty
    } else {

      val sourceIncrementalColumn: String = tableConf.source_increment_column
      val sourceIncColumnDataType: DataType = readKuduTableWithColumns(
        tableName = s"${tableConf.source_database}.${tableConf.source_table}",
        sourceIncrementalColumn,
        false.toString
      ).schema.head.dataType

      generateFilterCondition(
        sourceIncrementalColumn,
        incDF.schema.head.dataType,
        maxValueArr,
        sourceIncColumnDataType
      )
    }
  }

  /*
   * Validation for source based load
   *
   * @param tableConf table config
   * @return
   * */

  def getIncrementalColumnDf(tableConf: TableConfig): DataFrame = {

    val targetTable: String =
      s"${tableConf.target_database}.${tableConf.target_table}"

    val incrementalColumn: String = tableConf.target_increment_column
    val targetType: writerType = getTargetType(tableConf.target_table_type)

    if (!isSourceBasedLoad(tableConf.source_system)) {
      getIncrementalValueNonSourceBased(
        targetTable,
        targetType,
        incrementalColumn
      )
    } else {
      getIncrementalValueBasedOnSourceSystems(
        tableConf,
        targetTable,
        incrementalColumn
      )
    }
  }

  override def getSourceSystemsFromSourceData(
    tableConf: TableConfig
  ): Array[String] = {

    val sourceColumns: Array[String] = readKuduWithCondition(
      tableName = s"${tableConf.source_database}.${tableConf.source_table}",
      where = false
    ).columns

    val sourceSystems: Array[String] =
      if (sourceColumns
            .contains(sourceSystem))
        readKuduTable(
          tableName =
            s"(SELECT DISTINCT $sourceSystem FROM ${tableConf.source_database}.${tableConf.source_table})"
        ).collect()
          .map(_.getAs[String](0))
          .filter(_ != null)
      else
        Array[String](tableConf.source_system)

    sourceSystems.map(source => s"'$source'")
  }

}
