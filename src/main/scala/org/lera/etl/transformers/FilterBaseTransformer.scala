package org.lera.etl.transformers

import org.apache.log4j.Logger
import org.apache.spark.sql.types.{DataType,StringType,TimestampType,IntegerType}

trait FilterBaseTransformer extends BaseTransformer{
  
  val inConditions : Array[String] = Array("in","not in")
  
  implicit val filterEncoder : Encoder[FilterData]=
    Encoders.product[FilterData]
  
  val likeConditions : Array[String] = Array("like","not like")
  
  val getValue : FilterData => DataType => String = filterData =>
    filterData.filter_condition match {
      
      case condition if inConditions.contains(condition.toLowerCase) => {
        case StringType =>
          val inValue = filterData.filter_value
            .split(StringExpr.comma)
            .map(_.toLowerCase)
            .map(value => s"'$value'")
            .mkString(StringExpr.comma)
          s"($inValue)"
          
        case _ => s"(${filterData.filter_value})"
      }
      
      case condition if likeConditions.contains(condition.toLowerCase) =>
        _ =>
          s"'${filterData.filter_value}%'"
          
      case _ => {
        case StringType => s"'${filterData.filter_value.toLowerCase}'"
        case _ => filterData.filter_value
      }
    }
    
  val getFilterValueFunc : Map[String,DataType] => FilterData => String =
    tableSchema =>
      filterIns =>
        tableSchema
          .getOrElse(
            filterIns.filter_col_name.toLowerCase,
            throw new IBPException(
            s"Column ${filterIns.filter_col_name} not found in source data"    
            )
          ) match {
          
          case StringType | TimestampType =>
            getValue(filterIns)(StringType)
          case _ =>
            getValue(filterIns)(IntegerType)
        }
        
  private val logger : Logger = Logger.getLogger(this.getClass)
  
  protected def getFinalFilterCondition(source_table : String,
                                        groupPosition : Int,
                                        lastElement : FilterData,
                                        conditionExpr : String) : String = {
    
    val finalCondition : String =
      lastElement.logical_operator.toUpperCase match {
      
      case x if x.equals("AND") || x.equals("OR") =>
        s" ${StringExpr.startBrace}$conditionExpr${StringExpr.endBrace} $x"
        
      case _ =>
        s" ${StringExpr.startBrace}$conditionExpr${StringExpr.endBrace} AND"
        
    }
    
    logger.info(
    s"Filter condition for source system $source_table is $finalCondition in group $groupPosition"    
    )
    
    finalCondition
  }
  
  val getFilterColumn : String => Map[String,DataType] => String = column =>
    _.getOrElse(
    column.toLowerCase,
    throw new IBPException(
    s"Column $column not found in source data"    
    )
    ) match {
      case StringType => s"LOWER($column)"
      case _          => column
    }
  
}

case class FilterData(filter_col_name : String,
                      filter_condition : String,
                      filter_value : String,
                      logical_operator : String,
                      filter_order : Int,
                      group_order : Int)