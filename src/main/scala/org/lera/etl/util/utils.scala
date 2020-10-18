package org.lera.etl.util

import java.sql.Timestamp
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, trim}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.{DataFrame, Encoder, Row}
import org.lera.etl.Writers

import scala.collection.GenSeq
import scala.collection.parallel.ParSeq
import scala.util.{Failure, Success, Try}

/** 
 *  Utility methods and classes for IBP
 *  **/

object utils {
  
  lazy val columnMappingConfigTableName : String = {
    
    val ibpColumnMappingTable : String = getProperty(ibpColumnMappingTableName)
    s"$ibpConfigDatabase.$ibpColumnMappingTable"
  }
  
  lazy val defaultValuesConfigTableName : String = {
    
    val defaultTableName : String = getProperty(ibpDefaultValuesTable)
    s"$ibpConfigDatabase.$defaultTableName"
  }
  
  lazy val joinConfigTableName : String ={
    
    val joinTableName : String = getProperty(ibpJoinTable)
    s"$ibpConfigDatabase.$joinTableName"
  }
  
  lazy val lookupConfigTableName : String = {
    
    val lookupTableName : String = getProperty(ibpLookupTable)
    s"$ibpConfigDatabase.$lookupTableName"
  }
  
  lazy val conditionalMappingConfigTableName : String = {
    
    val condTableName : String = getProperty(ibpConditionTable)
    s"$ibpAuditDatabase.$condTableName"
  }
  
  lazy val deleteColumnConfigTableName : String = {
    
    val deleteTable : String = getProperty(ibpDeleteTable)
    s"$ibpConfigDatabase.$deleteTable"
  }
  
  lazy val filterColumnTableName : String = {
    
    val filterTable : String = getProperty(ibpFilterTable)
    s"$ibpConfigDatabase.$filterTable"
  }
  
  lazy val startTime : Timestamp = now
  val ibpConfigDatabase : String = getProperty(configDB)
  val ibpConfigTable : String = getProperty(configTableName)
  val ibpAuditDatabase : String = getProperty(auditDB)
  val ibpAuditTable : String = getProperty(ibpAuditTbl)
  
  val mapSQLValue : Map[String, String] => String =
    _.mapValues(value => s"'$value'")
      .map(tuple => s"${tuple._1}=${tuple._2}")
        .mkString(sqLAND)
        
  val getTargetType : String => Writers.writerType = tableType =>
    Writers
      .fromString(writerType = tableType)
      .getOrElse(throw new IBPException("unknown target type"))
      
  val getTableType : String => Writers.writerType = tableType =>
    Writers
      .fromString(writerType = tableType)
      .getOrElse(throw new IBPException("unknown target type"))
      
  val auditUpdate : (TableConfig,RunStatus) => Unit = (config,runState) => {
    ibpAuditTableUpdate(
    tableRunInfo = TableRunInfo(
    sourceSystem = config.source_system,
    region = config.sourcedata_regionname,
    tableName = config.target_table,
    loadType = config.load_type,
    status = runState,
    config.message
    )    
    )
  }
  
  val ibpAuditTableName : String = s"$ibpAuditDatabase.$ibpAuditTable"
  private val logger : Logger = Logger.getLogger(clazz= this.getClass)
  
  def selectSQLColumns(values : String*): String =
    values.mkString(StringExpr.comma)
  
  def readSQLFromHDFSFile(filepath : String) : String ={
    getSparkSession.read.textFile(filepath).collect().mkString
  }
    
  def insert[T](list : Seq[T], i:Int, value : T)  : Seq[T] = {
    
    list.take(i) ++ Seq(value) ++ list.drop(i)
  }
  
  def isSourceEnabled(property : String, source : String) : Boolean = 
    getConf
      .getOption(key = property)
      .getOrElse(StringExpr.empty)
      .trim
      .split(regex=StringExpr.comma)
      .map(_.trim.toLowerCase)
      .contains(source.toLowerCase)
      
  def isSourceBasedLoad(sourceSystem : String) : Boolean =
    getConf
      .getOption(targetTruncateKey)
      .getOrElse(empty)
      .trim
      .split(regex=comma)
      .filterNot(_.isEmpty)
      .map(_.toLowerCase)
      .contains(sourceSystem.toLowerCase)
      
  def PropertyException : String => Throwable =
    error => new MissingPropertyException(error)
    
  /* 
   * Generate JDBC URL by replacing username and password in the base URL
   * 
   * @param baseURL base URL
   * @param userName username
   * @param password password
   * @return
   * 
   * */
    
  def JDBC_URL_Generator(baseURL : String, userName : String, password : String) : String = {
      
      baseURL
        .replace(target = Constants.userName, replacement = userName)
        .replace(target = Constants.password, replacement = password)
    }
    
  def readHiveTable(tableName : String) : DataFrame =
    getSparkSession.table(tableName)
    
  /* 
   * Get count of CSV files in HDFS location
   * 
   * @param hdfsFileLocation File location
   * @return Number of files
   * */
    
  def getNumberOfFiles(hdfsFileLocation : String) : Int = {
    
    val fs : FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val filepath : String = hdfsFileLocation.replace(
        target = hdfsFileLocation.split(regex = StringExpr.slash).last,
        replacement = StringExpr.empty
    )
    
    val fileNameStartsWith : String = hdfsFileLocation
      .split(regex = StringExpr.slash)
      .last
      .split(regex = StringExpr.underscore)(0)
      
    val fileNameEndWith : String =
      hdfsFileLocation.split(regex = StringExpr.slash).last.split(regex = StringExpr.dot).last
      
    val locationDepth : Int =
      s"$hdfsFileLocationPrefix$filepath".split(regex = StringExpr.slash).length
      
    fs.listStatus( path = new Path(pathString = filepath))
      .filterNot(_.isDirectory)
      .map(_.getPath.toString.split(regex = StringExpr.slash)(locationDepth))
      .filter(_.endsWith(fileNameEndWith))
      .count(_.startsWith(fileNameStartsWith))
  }
  
  /* 
   * Puts run status into audit table
   * 
   * @param tableRunInfo table info
   * */
  
   def ibpAuditTableUpdate(tableRunInfo : TableRunInfo): Unit = {
     
     //TableRunInfo(sourceSystem : String, tableName : String, loadType : String, status : RunStatus, errorMessage : String = "")
     
     val TableRunInfo(source, regionName, table, loaderType, runInfo, message) =
       tableRunInfo
     
     val query : String = runInfo match{
       case RUNNING =>
         s"INSERT INTO TABLE $ibpAuditTableName VALUES('$source','$regionName','$table','$loaderType','$startTime','','$RUNNING','');"
         
       case SUCCESS =>
         s"UPDATE $ibpAuditTableName SET run_status = '$SUCCESS', end_time = '$now', message = '$message' WHERE table_name = '$table' AND run_status = 'RUNNING';"
       
       /*case TableRunInfo(_,_,table,_,FAILED, error) => 
        * s"UPDATE $ibpAuditTableName SET run_status = '$FAILED', end_time = '$now', message = '$error' WHERE table_name = '$table' AND run_status = 'RUNNING';"
        * */
         
       case FAILED =>
         s"UPSERT INTO TABLE $ibpAuditTableName VALUES('$source','$regionName','$table','$loaderType','$startTime','$now','$FAILED','$message');"
     }
     
     executeQuery(queries = query)
   }
   
   def getJobStatus(sourceConf : ParSeq[TableConfig]): Array[TableRunInfo] = {
     
     implicit val TableRunInfoEncoder : Encoder[TableRunInfo] =
       org.apache.spark.sql.Encoders.kryo[TableRunInfo]
     
     val tableConf : TableConfig = sourceConf.head 
     val auditDf : DataFrame = KuduUtils.readKuduWithCondition(
       ibpAuditTableName,
       where = s"source_system='${tableConf.source_system}' AND sourcedata_regionname = '${tableConf.sourcedata_regionname}'" +
         s"AND start_time = '$startTime' AND end_time <= '$now'"
     )
     
     auditDf
       .map(
         func = (row : Row) =>
           TableRunInfo(
             sourceSystem = row.getAs(fieldName = sourceSystem),
             region = row.getAs(fieldName = sourceDataRegionName),
             tableName = row.getAs(fieldName = "table_name"),
             loadType = row.getAs(fieldName = loadType),
             status = RunStatus.fromString(runStatus = row.getAs(fieldName = "run_status")).get,
             errorMessage = row.getAs(fieldName = "message")
             
           )
       )
       .collect()
       .filter(_.status == FAILED)
   }
   
   def now : Timestamp = new Timestamp(Calendar.getInstance().getTime.getTime)
   
   /* 
    * Null values replaced with default values in dataset
    * 
    * @param dataSet data set
    * @return
    * 
    * */
   
   def cleanDataFrame(dataSet: DataFrame) : DataFrame = {
     dataSet.na.fill(valueMap = typeMap(df = dataSet))
   }
   
   /* 
    * Default values for each data type
    * 
    * @param df dataframe
    * @return
    * 
    * */
   
   def typeMap(df : DataFrame) : Map[String, Any]= {
     df.dtypes
       .map(
         column =>
           column._2.toLowerCase match {
             case _ => column._1 -> "NULL"
           }
       )
       .toMap 
   }
   
   /* 
    * Get target table schema
    * 
    * @param tableConf table property
    * @return
    * */
   
   def getTablesSchema(tableConf : TableConfig) : Map[String, DataType] = {
     val tableName : String = s"${tableConf.target_database}.${tableConf.target_table}"
     readTableSchema(tableName)
   }
   
   /* 
    * Read schema from target table
    * 
    * @param tableName table name
    * @return
    * */
   
   def readTableSchema(tableName : String) : Map[String, DataType] = {
     
     val queryForSchema : String = s"(SELECT * FROM $tableName WHERE 1=0) _schema"
     
     KuduUtils
       .readKuduTable(tableName = queryForSchema)       
       .schema
       .map(structType => {
         (structType.name, structType.dataType)
       })
       .toMap
   }
   
   // Read partition column from table
   def readPartitionColumns(tableName : String) : String = {
     val columnNames : Array[String] = ContextCreator.getSparkSession
       .sql(sqlText = s"DESC $tableName")
       .select(col = "COL_NAME")
       .collect()
       .map(_.getString(0))
       
     if(columnNames
         .exists(x => x.trim.contains("Partition Information"))) {
       val partitionInfo : (Boolean, String) = columnNames.foldRight(
       (true, StringExpr.empty)    
       )((column, partitionTup) => {
         val (isPartition: Boolean, outColumn : String) = partitionTup
         if(isPartition && !column.trim.isEmpty){
           if(column.trim.equalsIgnoreCase(anotherString = "# col_name") || column
               .contains("col_name")) {
             (false,outColumn)
           } else {
             (isPartition,s"$outColumn$column,")
           }
         } else{
           (isPartition,outColumn)
         }
       })
       partitionInfo._2.substring(0,partitionInfo._2.length-1)
     } else StringExpr.empty
   }
   
   def handler[B](tableConf : TableConfig)(block : => B) : Seq[B] = {
     
     val out : Try[B] = Try(block)
     out match {
       case Success(outValue) => Seq(outValue)
       case Failure(exception) =>
         val trimErrorMessage : String = exception.getMessage 
           .replace(target = StringExpr.quote, replacement = StringExpr.empty)
           .split(regex = StringExpr.line)(0)
         val updatedTableConf : TableConfig = 
           tableConf.copy(message = tableConf.message + trimErrorMessage)
         auditUpdate(updatedTableConf, FAILED)
         exception.printStackTrace()
         logger.error(
         s"Data load has failed for ${tableConf.target_table} due to ${exception.getMessage}"    
         )
         Seq.empty 
     }
   }
   
   def errorHandler[A <: TableConfig , B](
     tableConf : A    
   )(block : => B) : Option[(A,B)] = {
     
     val out : Try[B] = Try(block)
     out match {
       case Success(result) => Some(tableConf,result)
       case Failure(exception) =>
         val trimErrorMessage : String = exception.getMessage
           .replace(target = StringExpr.quote, replacement = StringExpr.empty)
           .split(regex = StringExpr.line)(0)
         
         val updatedTableConf : TableConfig =
           tableConf.copy(message = tableConf.message + trimErrorMessage)
           
         auditUpdate(updatedTableConf, FAILED)
         exception.printStackTrace()
         logger.error(
         s"Data load has failed for ${tableConf.target_table} due to ${exception.getMessage}"    
         )
         None
           
     }
   }
  
   /*
    * Tuple utilities for mapping key and values
    * 
    * @param tup constructor type as tuple which has generic key and value type
    * @param T tuple's key type
    * @param V tuple's value type
    *  */
   
   implicit class TupleUtility[T,V](tup : (T,V)){
     /* 
      * Map tuple's values to pass by function
      * 
      * @param f function passed as parameter
      * @tparam B type need to be converted
      * @return
      * */
     
     def mapValues[B](f: V => B) : (T,B) = {
       
       val data : B = f(this.tup._2)
       (tup._1, data)
     }
     
     /* 
      * Maps tuple keys to pass by function
      * 
      * @param f function passed in parameter
      * @tparam B key type to be replaced
      * @return
      * */
     
     def mapKeys[B](f : T => B) : (B,V) = {
       
       val data : B = f(this.tup._1)
       (data, tup._2)
     }
   }
   
   implicit class DataFrameUtils(dataFrame : DataFrame) = {
     /* 
      * Remove trailing spaces in df string data
      * 
      * @return
      * */
     
   def trimColumnTrailingSpace : DataFrame = {
     
     dataFrame.schema
       .map(structField => {
         (structField.name,structField.dataType)
       })
       .filter(_._2 == StringType)
       .map(_._1)
       .foldRight(dataFrame)((columnName, df) => {
         df.withColumn(colName = columnName, col = trim(col(colName = columnName)))
       })
   }
   }
   
   implicit class OptionUtils[+A](option : Option[A]){
     
     /* 
      * String type is empty returns default value
      * 
      * @param default default value passes
      * @tparam B data type
      * @return
      * */
     
   def getNonEmptyOrElse[B >: A](default : => B): B = option match{
     case Some(x) =>
       if(x.isInstanceOf[String] && x.toString().trim.isEmpty) {
         default
       } else {
         x
       }
     case None => default
   }
   }
   
   /* 
    * Sequence utility for tuple
    * 
    * @param seq Constructor element as Sequence type which has tuple type
    * @tparam T tuple's key type
    * @tparam V tuple's value type
    * */
   
   implicit class SeqUtility[T,V](seq: GenSeq[(T,V)]) {
     /* 
      * Sequence of tuples mapped based on function
      * 
      * @param f function passed as parameter
      * @tparam B type need to be converted
      * @return
      * */
     
     def mapValues[B](f : V => B) : ParSeq[(T,B)] = {
       seq.map(_.mapValues(f)).par 
     }
     
     /* 
      * Sequence data tuple's keys mapped based on input function
      * @param f function passed as parameter
      * @tparam B key type to be replaced
      * @return
      * */
     
     def mapKeys[B](f : T => B) : ParSeq[(B,V)] = {
       seq.map(_.mapKeys(f)).par 
     }
     
     /* 
      * Filter keys from tuples
      * 
      * @return
      * */
     
     def keys : ParSeq[T] = {
       seq.map(_._1).par 
     }
     
     /* 
      * Filter values from tuples
      * 
      * @return
      * */
     
     def values : ParSeq[V] = {
       seq.map(_._2).par
     }
   }
}

class MissingPropertyException(message : String) extends Exception(message)

/*
 * Custom exception class for IBP
 * 
 * @param message error message
 *  
 */

class IBPException(message : String) extends Exception(message)

/* 
 * TableRunInfo is for IBP audit table
 * 
 * @param sourceSystem source system name
 * @param tableName    table name
 * @param loadType     load type
 * @param status       run status
 * @param errorMessage error message
 * */

case class TableRunInfo(sourceSystem : String,
                        region : String,
                        tableName : String,
                        loadType : String,
                        status : RunStatus,
                        errorMessage : String = "")