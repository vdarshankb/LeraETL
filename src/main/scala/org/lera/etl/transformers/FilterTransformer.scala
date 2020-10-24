package org.lera.etl.transformers

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType
import org.lera.TableConfig
import org.lera.etl.util.Constants._
import org.lera.etl.util.utils._

import scala.collection.parallel.ParSeq

object FilterTransformer extends FilterBaseTransformer{
  
  private val logger : Logger = Logger.getLogger(FilterTransformer.getClass)
  
  /* 
   * Each implementation provides the different transformation logic based on sources
   * @param dataFrameSeq tuples of table config and dataFrame
   * @return
   * 
   * */
  
  override def transform(
    dataFrameSeq : ParSeq[(TableConfig, DataFrame)]    
  ) : ParSeq[(TableConfig, DataFrame)] ={
    
    val filterConfigDataFrame : DataFrame =
      configHandler(dataFrameSeq.keys)(getTableConfigFunc(filterColumnTableName))
      dataFrameSeq.flatMap((confDataSetTup : (TableConfig, DataFrame)) => {
        val (conf,df)  = confDataSetTup
        
        errorHandler(conf){
          val source_table : String = conf.source_table
          logger.info(s"Filter transformer started for the table $source_table")
          
          val tableSchema : Map[String, DataType] = df.schema
            .map(field => {
              (field.name.toLowerCase,field.dataType)
            })
            .toMap
          
          val groupedFilters : Seq[(Int,Seq[FilterData])] =
            getFilterConfig(conf,filterConfigDataFrame)
          
          val conditionalExpr : String =
            getConditionalExpr(source_table, tableSchema, groupedFilters)
          
          if(conditionalExpr.isEmpty) df
          else
            df.where(conditionalExpr)
        }
      })
    
  }
  
  def getConditionalExpr(
  source_table : String,
  tableSchema : Map[String, DataType],
  groupedFilters : Seq[(Int, Seq[FilterData])]
  ) : String = {
    
    val getFilterValue : FilterData => String =
      getFilterValueFunc(tableSchema)
    
    val getFilterColumns : String => String = getFilterColumn(_)(tableSchema)
    
    val conditionalExpr : String = groupedFilters
      .map(tup => {
        val conditionCheckFunc : FilterData => String = filterDataIns =>
          s"${getFilterColumns(filterDataIns.filter_col_name)} ${filterDataIns.filter_condition} ${getFilterValue(filterDataIns)}"
          
        val condExpr : String = tup._2 
          .dropRight(1)
          .foldLeft(StringExpr.empty)(
          (startCond : String, filterDataIns : FilterData) => {
            s"${startCond} ${conditionCheckFunc(filterDataIns)} ${filterDataIns.logical_operator}".trim
          }    
          )
          
        val lastFilterDataIns : FilterData = tup._2.last 
        
        val conditionExpr : String =
          s"${condExpr} ${conditionCheckFunc(lastFilterDataIns)}".trim 
          
        getFinalFilterCondition(
        source_table,
        tup._1,
        lastFilterDataIns,
        conditionExpr
        )
      })
      .mkString(StringExpr.empty)
      
      val finalCondition : String = if(conditionalExpr.nonEmpty) {
        conditionalExpr.substring(0, conditionalExpr.length - 4)
      } else StringExpr.empty
      
      logger.info(
      s"Final filter condition for source system $source_table is $finalCondition"    
      )
      finalCondition.trim 
  }
  
  def getFilterConfig(
      tableConf : TableConfig,
      filterConfigDataFrame : DataFrame
      ) : Seq[(Int, Seq[FilterData])]= {
    val source_table : String = 
      tableConf.source_table
    val whereQuery : String =
      Map(
      sourceTable -> source_table    
      ).toWhereCondition.ignoreCaseInSQL

    val filterDataArray : Array[FilterData]= filterConfigDataFrame
    .where(whereQuery)
    .selectExpr( "filter_col_name",
                 "filter_condition",
                 "filter_value",
                 "logical_operator",
                 "filter_order",
                 "group_order").as[FilterData].collect()
    
    filterDataArray
    .groupBy(_.group_order)
    .mapValues(value => value.toSeq.sortBy(_.filter_order))
    .toSeq
    .sortBy(_._1)
                 
  }
  
}