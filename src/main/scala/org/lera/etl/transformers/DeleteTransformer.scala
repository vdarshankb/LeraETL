package org.lera.etl.transformers

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.lera.TableConfig
import org.lera.etl.util.Constants._
import org.lera.etl.util.utils._

import scala.collection.parallel.ParSeq
object DeleteTransformer extends FilterBaseTransformer {

  import org.apache.spark.sql.types.DataType


  val getDeleteCond: String => String = {
    case ">"        => "<"
    case ">="       => "<="
    case "<"        => ">"
    case "<="       => ">="
    case "="        => "!="
    case "!="       => "="
    case "like"     => "not like"
    case "not like" => "like"
    case "in"       => "not in"
    case "not in"   => "in"

    case x => throw new Exception(s"Unknown delete condition $x")
  }

  private val logger: Logger = Logger.getLogger(DeleteTransformer.getClass)

  /*
   * Each implementation provides a different transformation logic based on sources
   * Condition added to make sure deletion happens using same case. This will not work for INT
   * @param dataFrameSeq tuples of table config and dataFrame
   * @return
   *
   *  */

  override def transform(
    dataFrameSeq: ParSeq[(TableConfig, DataFrame)]
  ): ParSeq[(TableConfig, DataFrame)] = {
    val deleteConfigDataFrame: DataFrame =
      configHandler(dataFrameSeq.keys)(
        getTableConfigFunc(deleteColumnConfigTableName)
      )

    dataFrameSeq.flatMap((dataset: (TableConfig, DataFrame)) => {
      val (conf, df): (TableConfig, DataFrame) = dataset
      errorHandler(conf) {
        import org.apache.spark.sql.types.DataType
        logger.info(
          s"Delete transformer started for the table ${conf.source_table}"
        )
        val tableSchema: Map[String, DataType] = df.schema
          .map(field => {
            (field.name.toLowerCase(), field.dataType)
          })
          .toMap

        val groupedDeletes: Seq[(Int, Seq[FilterData])] =
          getDeleteConfig(conf, deleteConfigDataFrame)

        val conditionalExpr: String =
          getConditionalExpr(conf.source_table, tableSchema, groupedDeletes)

        if (conditionalExpr.isEmpty) df
        else df.where(conditionalExpr)
      }
    })

  }

  def getConditionalExpr(
    source_table: String,
    tableSchema: Map[String, DataType],
    groupedDeletes: Seq[(Int, Seq[FilterData])]
  ): String = {

    val getFilterValue: FilterData => String = getFilterValueFunc(tableSchema)
    val getFilterColumns: String => String = getFilterColumn(_)(tableSchema)
    val conditionalExpr =
      groupedDeletes
        .map(tup => {
          val conditionCheckFunc: FilterData => String = filterDataIns =>
            s"${getFilterColumns(filterDataIns.filter_col_name)} ${getDeleteCond(filterDataIns.filter_condition)} ${getFilterValue(filterDataIns)}"

          val condExpr = tup._2
            .dropRight(1)
            .foldLeft(StringExpr.empty)((a, b) => {
              s"$a ${conditionCheckFunc(b)} ${b.logical_operator}".trim
            })

          val lastDeleteDataIns: FilterData = tup._2.last

          val conditionExpr: String =
            s"$condExpr ${conditionCheckFunc(lastDeleteDataIns)}".trim

          getFinalFilterCondition(
            source_table,
            tup._1,
            lastDeleteDataIns,
            conditionExpr
          )
        })
        .mkString(StringExpr.empty)

    val finalCondition: String = if (conditionalExpr.nonEmpty) {
      conditionalExpr.substring(0, conditionalExpr.length - 4)
    } else StringExpr.empty

    logger.info(
      s"Final delete condition for source system $source_table is $finalCondition"
    )
    finalCondition.trim

  }

  def getDeleteConfig(
    tableConf: TableConfig,
    configDataFrame: DataFrame
  ): Seq[(Int, Seq[FilterData])] = {

    val source_table =
      tableConf.source_table
    val whereQuery: String =
      Map(sourceTable -> source_table).toWhereCondition.ignoreCaseInSQL

   import spark.implicits._


    val deleteDataArray: Array[FilterData] = configDataFrame
      .where(whereQuery)
      .select(
        $"delete_col_name".as( "filter_col_name"),
        $"delete_condition".as( "filter_condition"),
        $"delete_value".as( "filter_value"),
        $"logical_operator",
        $"delete_order".as( "filter_order"),
        $"group_order"
      )
      .as[FilterData]
      .collect()

    deleteDataArray
      .groupBy(_.group_order)
      .mapValues(value => value.toSeq.sortBy(_.filter_order))
      .toSeq
      .sortBy(_._1)
  }

}
