package org.lera.etl.readers

import java.io.FileNotFoundException

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, TimestampType}
import org.lera.TableConfig
import org.lera.etl.util.Constants
import org.lera.etl.util.Constants.StringExpr
object SQLKuduReader extends Reader{
  
  private val logger : Logger = Logger.getLogger(SQLKuduReader.getClass)
  
  private val impalaDialect : JdbcDialect = new JdbcDialect{
    
    override def canHandle(url : String) : Boolean = {
      url.toLowerCase.startsWith("jdbc:impala") || url.toLowerCase.contains(
      "impala"    
      )
      
    override def quoteIdentifier(colName : String) : String = {
        s""
      }
    }
    
    JdbcDialects.registerDialect(impalaDialect)
    
    /* 
     * Read data from source
     * 
     * @param tableConf table properties
     * @return
     * */
    
    override def readData(tableConf : TableConfig) : (TableConfig, DataFrame) = {
      
      val filePath : String = tableConf.file_path
      
      if(!isFilePathExists(filePath)) {
        throw new FileNotFoundException(
        s"File does not exist in : ${tableConf.source_table}"    
        )
      }
      
      val query = 
        readSQLFromHDFSFile(filePath)
        
      val tableName = tableConf.source_table
      
      val dataFrame : DataFrame = tableConf.load_type.toLowerCase match {
        
        case Constants.incremental | Constants.incr =>
          val targetTable = 
            s"${tableConf.target_database}.${tableConf.target_table}"
            
          val targetIncrementalColumn = tableConf.target_increment_column
          val sourceIncrementalColumn = tableConf.source_increment_column
          
          val maxTargetCond : String = s"max($targetIncrementalColumn)"
          import org.lera.etl.util.Enums.Writers._
          val maxValueArr : DataFrame = getTargetType(tableConf.target_table_type) match {
            
            case HIVE =>
              readHiveTable(targetTable).selectExpr(maxTargetCond)
              
            case _ =>
              readKuduTableWithColumns(targetTable,maxTargetCond)
          }
          
          val targetType : DataType = maxValueArr.schema.head.dataType
          
          val maxVal : Array[Row] = 
            maxValueArr.filter(row => null != row.getAs(0)).collect()
            
          val sourceType : DataType = readKuduTableWithColumns(
          tableName = s"${tableConf.source_database}.$tableName",
          sourceIncrementalColumn,
          false.toString
          ).schema.head.dataType
          
          val cond = if(maxVal.isEmpty) {
            StringExpr.empty
          } else {
            (sourceType,targetType) match {
              case Tuple2(LongType,TimestampType) =>
                s"$sourceIncrementalColumn > CAST(CAST('${maxVal(0).get(0)}' AS TIMESTAMP) AS )"
                
              case (LongType | IntegerType , _) => s"$sourceIncrementalColumn > ${maxVal(0).get(0)}" 
              case _ => s"$sourceIncrementalColumn > '${maxVal(0).get}'"
            }
          }
          
          logger.info(s"Incremental condition is $cond")
          
          readKuduTableWithQuery(
          tableConf.source_system,
          query,
          tableConf.target_database,
          tableName,
          cond
          )
          
        case Constants.fullLoadType =>
          readKuduTableWithQuery(
          tableConf.source_system,
          query,
          tableConf.target_database,
          tableName
          )
      }
      
      (tableConf,dataFrame)
    }
    
  }
  
}