package org.lera.etl.readers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.{DataType, LongType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}
import org.lera.etl.util.Constants.{StringExpr, _}
import org.lera.etl.util.Enums.Writers
import org.lera.etl.util.Enums.Writers._
import org.lera.etl.util.KuduUtils._
import org.lera.etl.util.utils._
import org.lera.{ContextCreator, TableConfig}

trait Reader extends ContextCreator {

  /*
   *
   * Read data from source
   * @Param properties file
   * @return
   */

  def readData(properties: TableConfig): (TableConfig,DataFrame)

  def isFilePathExists(path: String): Boolean = {
    FileSystem.get(new Configuration()).exists(new Path(path))

  }

  def jsonToDataFrame(jsonString: Seq[String]): DataFrame = {
    import spark.implicits._
    spark.read.json(jsonString.toDS)
  }

  def getIncrementalValueBasedOnSourceSystems(
    tableConf: TableConfig,
    targetTable: String,
    incrementalColumn: String
  ): DataFrame = {

    import org.lera.etl.util.Enums.Writers.writerType
    val targetType: writerType = getTargetType(tableConf.target_table_type)
    val sourceAndRegionList: Array[String] = getSourceSystemsFromSourceData(
      tableConf
    )
    val sourceSystems: String = sourceAndRegionList.mkString(",")
    val whereCondition: String =
      if (sourceSystems.nonEmpty)
        s"$sourceSystems IN ($sourceSystems) AND $incrementalColumn IS NOT NULL"
      else
        s"$incrementalColumn IS NOT NULL"
    val selectColumns: String = s"$incrementalColumn , $sourceSystems"
    val maxValueDataFrame: DataFrame =
      readTargetMaxValue(targetType, targetTable, selectColumns, whereCondition)
    maxValueDataFrame
      .groupBy(sourceSystem)
      .agg(max(incrementalColumn).as(incrementalColumn))
      .select(min(incrementalColumn))

  }

  def getSourceSystemsFromSourceData(tableConf: TableConfig): Array[String] = {
    import org.lera.etl.util.ETLException
    throw new ETLException(
      s"Code implementation to get source system and regions is missing in $this"
    )

  }

  protected def generateFilterCondition(
    sourceIncrementalColumn: String,
    targetIncColumnDataType: DataType,
    maxValueArr: Array[Row],
    sourceIncColumnDataType: DataType
  ): String = {
    (sourceIncColumnDataType, targetIncColumnDataType) match {

      case (LongType, TimestampType)       => s""
      case (StringType | TimestampType, _) => s""
      case _                               => s""
    }
  }

  /*
   * Read max value for the incremental load
   * @param targetType table type
   * @param targetTable table name
   * @param columns column name
   * @param condition where condition
   * @return
   *
   * */

  protected def getIncrementalValueNonSourceBased(
    targetTable: String,
    targetType: writerType,
    incrementalColumn: String
  ): DataFrame = {

    val maxTargetColumn: String = s"max ($incrementalColumn)"

    readTargetMaxValue(
      targetType,
      targetTable,
      maxTargetColumn,
      condition = s"$incrementalColumn IS NOT NULL"
    )
  }

  protected def readTargetMaxValue(targetType: Writers.writerType,
                                   targetTable: String,
                                   columns: String,
                                   condition: String): DataFrame = {

    targetType match {
      case HIVE =>
        spark
          .table(targetTable)
          .where(condition)
          .selectExpr(columns.split(StringExpr.comma): _*)

      case _ => readKuduTableWithColumns(targetTable, columns, condition)

    }
  }

}
