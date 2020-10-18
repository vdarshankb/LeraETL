package org.lera.etl.Writers

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.parallel.immutable.ParSeq

object KuduWriter extends Writer {

  private val logger: Logger = Logger.getLogger(KuduWriter.getClass)

  /*
   * Persist data into Kudu table
   *
   * @param updatedData data set and table properties
   * */

  override def write(updatedData: ParSeq[(TableConfig, DataFrame)]): Unit = {

    updatedData.foreach(tuple => {

      val (tableConf: TableConfig, df: DataFrame) = tuple
      val target_table: String = tableConf.target_table
      val load_type: String = tableConf.load_type.toLowerCase

      errorHandler(tuple._1) {

        val tableName: String = s"${tableConf.target_database}.$target_table"
        logger.info(s" load type for the table $tableName is $load_type")

        val isFullLoad: Boolean = load_type
          .contains(Constants.fullLoadType)

        if (isFullLoad)
          FullLoadValidations(df, tableConf.source_system, tableName)

        val intermediateTableName: String = s"${tableName}_intermediate"
        //logger.debug(s"Saved in Hive temporary table $intermediateTableName")

        logger.info(
          s"Data persisted in hive intermediate table $intermediateTableName"
        )

        df.write
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName = s"$intermediateTableName")

        executeQuery(queries = s"INVALIDATE METADATA $intermediateTableName")
        logger.info(s"Loading data into target table $tableName")

        val query: String =
          buildQueryForInsert(intermediateTableName, tableName, df.columns)(
            isFullLoad
          )

        executeQuery(query)
        logger.info(s"Successfully loaded data into target table $tableName")
        auditUpdate(tableConf, SUCCESS)

      }
    })
  }

  private def FullLoadValidations(df: DataFrame,
                                  source_system: String,
                                  tableName: String): Boolean = {

    if (isSourceBasedLoad(source_system)) {

      val sourceAndRegions: Array[String] = getSourceSystemNames(df)
        .map(names => s"'$names'")

      val sourceSystems: String =
        sourceAndRegions.mkString(StringExpr.comma)

      /* val regions : String =
       * sourceAndRegions.map(_._2).mkString(StringExpr.comma)*/

      logger.info(s"Source systems $sourceSystems data to be overwritten")

      val filterCond =
        if (sourceAndRegions.nonEmpty)
          s"LOWER($sourceSystem) in ($sourceSystems)"
        else StringExpr.empty

      executeQuery(buildDeleteStatement(tableName, filterCond))
    } else {
      executeQuery(buildDeleteStatement(tableName))
    }

  }

}
