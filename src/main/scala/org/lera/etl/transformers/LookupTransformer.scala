package org.lera.etl.transformers

import org.apache.log4j.Logger
import scala.collection.parallel.immutable.ParSeq
import org.apache.spark.sql.DataFrame

/* 
 * Lookup transformer does Lookup transformation on input data sets
 * 
 * */

object LookupTransformer extends JoinBaseTransformer{
  
  private val logger : Logger = Logger.getLogger(LookupTransformer.getClass)
  
  /* 
   * Each implementation provides the different transformation logic based on sources
   * @param dataFrameSeq tuples of table config and dataFrame
   * @return
   * 
   * */
  
  override def transform(
    dataFrameSeq : ParSeq[(TableConfig, DataFrame)]    
  ) : ParSeq[(TableConfig, DataFrame)] ={
    
    val lookupConfigDataFrame : DataFrame =
      configHandler(dataFrameSeq.keys)(getTableConfigFunc(lookupConfigTableName))
    
    dataFrameSeq.flatMap(dataTup => {
      val (tableConf, sourceDf) = dataTup
      errorHandler(tableConf){
        val getJoinMap: Seq[(LookupGroup,Map[String,String])] =
          getLookupDetails(tableConf,lookupConfigDataFrame)
          
          lookupTransformations(tableConf, sourceDf, getJoinMap)
      }
    })
  }
  
  def lookupTransformations(tableConf : TableConfig,
                            sourceDf : DataFrame,
                            getJoinMap : Seq[(LookupGroup,Map[String,String])]) : DataFrame = {
    getJoinMap.foldLeft(sourceDf)(
    (df : DataFrame, lookupInfo : (LookupGroup,Map[String,String])) => {
      logger.info(
      s"Joining look up tables >> ${tableConf.source_table} with ${lookupInfo._1.look}"    
      )
      
      val sourceDf : DataFrame = nullReplaceDf(df)(lookupInfo._2.keys.toSeq)
      val lookUpDf : DataFrame = readLookupData(tableConf, lookupInfo)
      
      val seqCon : Column = conditionCreator(sourceDf)(lookUpDf)(lookupInfo._2)
      logger.info(
      s"Join condition for tables ${tableConf.source_table} and lookup table ${lookupInfo}"    
      )
      
      val dropColumns : Seq[Column] = getDropColumns(sourceDf)(lookUpDf)
      logger.info(s"Delete columns are $dropColumns")
      
      val joinedDf : DataFrame = sourceDf.join(lookUpDf, seqCon, joinType = "left")
      dropColumns.foldLeft(joinedDf)((df, column) => df.drop(column))
    }    
    )
  }
  
  /*
   * Read look up table and replace null values
   * @param tableConf table configuration
   * @param lookupInfo look up details
   * @return
   *  */
  
  def readLookupData(
  tableConf : TableConfig,
  lookupInfo : (LookupGroup, Map[String,String])
  ) : DataFrame = {
    import org.apache.spark.sql.expressions.{WindowSpec,Window}
    
    val lookupColumns : Seq[String] = lookupInfo._2.values.toSeq 
    val lookUpConfig : TableConfig = lookupInfo._1.getTableConfig(tableConf)
    val windowFunc : WindowSpec = 
      Window
      .partitionBy(lookupColumns.head, lookupColumns.tail : _*)
      .orderBy(lookupColumns.head)
      
    val lookUpDataFrame : DataFrame = readLookUpData(lookUpConfig)
      .withColumn(rowNumber, row_number().over(windowFunc))
      .where(conditionExpr = s"$rowNumber=1")
      .drop(rowNumber)
      
      nullReplaceDf(lookUpDataFrame)(lookupColumns)
      
  }
  
  /* 
   * Get look up table information
   * @param tableConf table properties
   * @return
   * */
  
  def getLookupDetails(
    tableConf : TableConfig,
    configDataFrame : DataFrame
  ) : Seq[(LookupGroup,Map[String,String])] = {
    
    val whereQuery : String =
      Map(
      sourceDB -> tableConf.source_database,
      sourceTable -> tableConf.source_table,
      targetDB -> tableConf.target_database,
      targetTable -> tableConf.target_table
      ).toWhereCondition.ignoreCaseInSQL
      
    val lookupConfigDf : Array[Lookup] = configDataFrame
      .where(whereQuery)
      .select(lookupDB,lookupTable,sourceColumn, lookupColumn, lookupTableType, lookupOrder)
      .as[Lookup](Encoders.product[Lookup])
      .collect()
      
      lookupConfigDf
      .groupBy(_.getLookupGroup)
      .mapValues(
      _.map(lookup => (lookup.source_column, lookup.lookup_column)).toMap    
      )
      .toSeq
      .sortBy(_._1.lookupOrder)  
      
  }
}

case class LookupGroup(lookupDatabase : String,
                        lookupTable : String,
                        lookupTableType : String,
                        lookupOrder : Int) {
  def getTableConfig(tableConfig : TableConfig) : TableConfig = {
    tableConfig.copy(
    source_database = lookupDatabase,
    source_table = lookupTable,
    source_table_type = lookupTableType,
    file_path = lookupTable,
    load_type = fullLoadType
    )
  }
}

case class Lookup( lookup_database : String,
                   lookup_table : String,
                   lookup_table_type : String,
                   source_column : String,
                   lookup_column : String,
                   lookup_order : Int) {
  def getLookupGroup: LookupGroup = 
    LookupGroup(lookup_database, lookup_table, lookup_table_type, lookup_order)
  
}