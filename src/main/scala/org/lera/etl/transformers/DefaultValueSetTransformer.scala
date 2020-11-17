package org.lera.etl.transformers

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType
import org.lera.TableConfig
import org.lera.etl.util.Constants._
import org.lera.etl.util.utils._

import scala.collection.parallel.ParSeq

/*
 * DefaultValueSetTransformer adds default values for target tables
 *
 *  */

object DefaultValueSetTransformer extends BaseTransformer {

  private val logger: Logger = Logger.getLogger(DefaultValueSetTransformer.getClass)

  logger.info("Inside the DefaultValueSetTransformer object")

  /*
   * @param dataFrameSeq input dataset
   * @return
   *
   * */

  override def transform(
    dataFrameSeq: ParSeq[(TableConfig, DataFrame)]
  ): ParSeq[(TableConfig, DataFrame)] = {
    val defaultConfigDataFrame: DataFrame =
      configHandler(dataFrameSeq.keys)(
        getDefaultTableConfigFunc(defaultValuesConfigTableName)
      )
    dataFrameSeq.flatMap(dataTup => {
      val (tableConf: TableConfig, sourceDf: DataFrame) =
        dataTup
      handler(tableConf) {
        logger.info(
          s"Default value set started for table >> ${tableConf.target_table}"
        )

        val defaultValueColumns: Array[(String, String)] =
          getDefaultValueConfig(tableConf, defaultConfigDataFrame)

        val mappingColumns: Array[String] =
          ColumnMappingTransformer.targetMappingColumns
            .getOrElse(tableConf.target_table, Array.empty)

        val defaultVal: Array[(String, String)] =
          defaultValueColumns.filterNot(
            columns => mappingColumns.contains(columns._1)
          )

        val invalidDefaultColumns: String = defaultValueColumns
          .filter(columns => mappingColumns.contains(columns._1))
          .mkString(StringExpr.comma)

        val tableSchema: Map[String, DataType] =
          targetTableSchema(tableConf)
            .map(field => {
              (field.name, field.dataType)
            })
            .toMap

        logger.info(s"Printing the table schema: ${tableSchema.seq.toString()}")

        val defaultValueSetDf: DataFrame = setDefaultValues(tableSchema, sourceDf, defaultVal)

        val updatedTableconf: TableConfig = updateTableConfig(tableConf, invalidDefaultColumns)

        (updatedTableconf, defaultValueSetDf)
      }
    })

  }

  def updateTableConfig(tableConf: TableConfig,
                        invalidDefaultColumns: String): TableConfig = {
    val updatedTableConf: TableConfig =
      if (!invalidDefaultColumns.isEmpty) {
        logger.error(
          s"Invalid default columns $invalidDefaultColumns for table >> $tableConf.target_table"
        )
        tableConf.copy(
          message = tableConf.message + "Invalid column names for default value set"
        )
      } else tableConf
    logger.info(
      s"Default value set completed for table >> $tableConf.target_table"
    )
    updatedTableConf
  }

  def setDefaultValues(
    tableSchema: Map[String, DataType],
    sourceDataFrame: DataFrame,
    columnDefaultValues: Array[(String, String)]
  ): DataFrame = {
    columnDefaultValues.foldRight(sourceDataFrame)((defaultColumnTup, df) => {
      val columnName: String =
        defaultColumnTup._1
      tableSchema
        .getOrElse(
          columnName,
          logger.warn(s"Target table does not have the default value column : $columnName")
        )

      val defaultValue: String =
        if (defaultColumnTup._2.trim.equalsIgnoreCase("null")) null
        else defaultColumnTup._2

      df.withColumn(columnName, lit(defaultValue))

    })
  }

  /*
   * Default columns and values pulled from Kudu meta tables
   * @param tableConf table properties
   * @return
   *
   *  */

  def getDefaultValueConfig(
    tableConf: TableConfig,
    configDataFrame: DataFrame
  ): Array[(String, String)] = {
    val whereQuery: String =
      Map(targetTable -> tableConf.target_table).toWhereCondition.ignoreCaseInSQL

    configDataFrame
      .where(whereQuery)
      .select(targetColumn, defaultValue)
      .collect()
      .map(row => {
        (
          row
            .getString(0)
            .trim
            .toLowerCase,
          row.getString(1)
        )
      })
  }

}
