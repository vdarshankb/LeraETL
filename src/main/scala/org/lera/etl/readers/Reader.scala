package org.lera.etl.readers

import java.util.Properties
import org.apache.spark.sql.{DataFrame,SparkSession,RuntimeConfig, Row}
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.spark.sql.types.{DataType,LongType,TimestampType,StringType}
import org.apache.spark.sql.functions.{min,max}


trait Reader {
  
  val conf : RuntimeConfig = ContextCreator.getConf()
  val spark : SparkSession = ContextCreator.getSparkSession() 
  
  /* 
   * 
   * Read data from source
   * @Param properties file
   * @return
   */
  
  def readData(properties : Properties, spark : SparkSession) : DataFrame
  
  def isFilePathExists(path : String) : Boolean = {
    FileSystem.get(new Configuration()).exists(new Path(path))
    
    
  } 
  
  def jsonToDataFrame(jsonString: Seq[String]) : DataFrame = {
    import spark.implicits._ 
    spark.read.json(jsonString.toDS)
  } 
  
  protected def generateFilterCondition(
  sourceIncrementalColumn : String,
  targetIncColumnDataType : DataType,
  maxValueArr : Array[Row],
  sourceIncColumnDataType : DataType
  ) : String = {
    (sourceIncColumnDataType,targetIncColumnDataType) match {
      
      case (LongType, TimestampType) =>  s""
      case (StringType | TimestampType, _) => s""
      case _ => s""
    } 
  }
  
  protected def getIncrementalValueNonSourceBased(
  targetTable : String,
 // targetType : writerType, Uncomment later when writerType is known
  incrementalColumn : String
  ) : DataFrame = {
    
    val maxTargetColumn : String = s"max ($incrementalColumn)"
    
    readTargetMaxValue(
 //   targetType, Uncomment later when writerType is known
    targetTable,
    maxTargetColumn,
    condition = s"$incrementalColumn IS NOT NULL"
    )
  } 
  
  def getIncrementalValueBasedOnSourceSystems(
  
      tableConf: TableConfig,
      targetTable : String,
      incrementalColumn : String    
  ) : DataFrame = {
    
   // import utils.Enums.Writer.writerType
    import utils.Enums.Writer.writerType
    
    val targetType : writerType             = getTargetType(tableConf.target_table_type)
    val sourceAndRegionList : Array[String] = getSourceSystemsFromSourceData(tableConf)
    val sourceSystems : String              = sourceAndRegionList.mkString(",")
    val whereCondition : String = 
      if(sourceSystems.nonEmpty)
        s"$sourceSystems IN ($sourceSystems) AND $incrementalColumn IS NOT NULL"
      else
        s"$incrementalColumn IS NOT NULL"
    val selectColumns : String = s"$incrementalColumn , $sourceSystems"
    val maxValueDataFrame : DataFrame = 
      readTargetMaxValue(targetType, targetTable,selectColumns,whereCondition)
    maxValueDataFrame
    .groupBy(sourceSystem)
    .agg(max(incrementalColumn).as(incrementalColumn))
    .select(min(incrementalColumn))
    
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
  
  protected def readTargetMaxValue(targetType : Writers.writerType,
  targetTable : String,
  columns : String,
  condition : String
  ) : DataFrame = {
    
    targetType match {
      case HIVE =>

        readHiveTable(targetTable)
        .where(condition)
        .selectExpr(columns.split(StringExpr.comma) : _*)
      
      case _ => readKuduTableWithColumns(targetTable,columns,condition)
        
    }
  } 
  
  def getSourceSystemsFromSourceData(
  tableConf : TableConfig    
  ) : Array[String] = {
    import org.lera.etl.util.IBPException
    throw new IBPException (s"Code implementation to get source system and regions is missing in $this")
    
  } 
  
}