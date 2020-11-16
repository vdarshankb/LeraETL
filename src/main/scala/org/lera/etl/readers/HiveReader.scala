package org.lera.etl.readers

import org.apache.log4j.Logger
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.lera.TableConfig
import org.lera.etl.util.Constants
import org.lera.etl.util.Constants.{StringExpr, _}
import org.lera.etl.util.Enums.Writers.writerType
import org.lera.etl.util.utils._

object HiveReader extends Reader{
  
  private val logger : Logger = Logger.getLogger(HiveReader.getClass)

  /*
   * 
   * Read data from source
   * @Param properties file
   * @return
   */
  
  
  override def readData(conf : TableConfig) : (TableConfig,DataFrame) = {
    
    val tableName = s"${conf.source_database}.${conf.source_table}"
    logger.info(s"Reading data from Hive table $tableName")
    
    val dataFrame : DataFrame = conf.load_type.toLowerCase match {
      case Constants.incremental | Constants.incr => 
        val incrementalCondition : String = getIncrementalLoadFilterCondition(
        conf    
        )
        logger.info(s"Incremental condition is $incrementalCondition")
        val sourceDataFrame : Dataset[Row] = readHiveTable(tableName)
        
        if(incrementalCondition.nonEmpty)
          sourceDataFrame.where(incrementalCondition)
        else
          sourceDataFrame
       
      case Constants.fullLoadType => readHiveTable(tableName)
      
    }
    
    (conf,dataFrame)
    
  } 
  
  
  
  private def getIncrementalLoadFilterCondition(
    tableConf : TableConfig    
  ): String = {
    
    val incDF : DataFrame = getIncrementalColumnDf(tableConf)
    
    val maxValueArr : Array[Row] = 
      incDF.filter(_.getAs(0) != null).collect()
      
    if(maxValueArr.isEmpty){
      
      
      StringExpr.empty
    }
    else {
      
      val sourceIncrementalColumn : String = tableConf.source_increment_column
      val sourceIncColumnDataType : DataType = readHiveTable(
        tableName = s"${tableConf.source_database}.${tableConf.source_table}"    
      ).selectExpr(sourceIncrementalColumn).schema.head.dataType
      
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
   * @param tableConf table config
   * @return
   * */
  
  
  def getIncrementalColumnDf(tableConf : TableConfig) : DataFrame = {
    
    val targetTable : String = 
      s"${tableConf.target_database}.${tableConf.target_table}"
    val incrementalColumn : String = tableConf.target_increment_column
    val targetType : writerType = getTargetType(tableConf.target_table_type)
    
    if(!isSourceBasedLoad(tableConf.source_system)){
      getIncrementalValueNonSourceBased(
        targetTable,
        targetType,
        incrementalColumn
      )
    }
    else {
      getIncrementalValueBasedOnSourceSystems(
        tableConf,
        targetTable,
        incrementalColumn
      )
    }
  } 
  
  
  override def getSourceSystemsFromSourceData(
    tableConf : TableConfig    
  ) : Array[String] = {
    
    val sourceColumns : Array[String] = readHiveTable(tableName = s"${tableConf.source_database}.${tableConf.source_table}"
    ).columns
    
    val sourceSystems : Array[String] = 
      if(sourceColumns
          .contains(sourceSystem))
        readHiveTable(tableName = s"${tableConf.source_database}.${tableConf.source_table}")
        .select(sourceSystem)
        .distinct()
        .collect()
        .map(_.getAs[String](0))
        .filter(_!=null)
      
       else 
         Array[String](
         tableConf.source_system    
         )
       
      sourceSystems.map(source => s"'$source'")
           
  } 
  
}