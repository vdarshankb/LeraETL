package org.lera.etl.transformers

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.lera.TableConfig

import scala.collection.parallel.immutable.ParSeq
import scala.concurrent.duration.Duration

trait BaseTransformer {
  
  val fiscalYearUDF : UserDefinedFunction = udf(
    (value : String, sourceFormat : String) => {

      dateValidator(value) ( value => {

        val cal : Calendar = getDateInstance(value, sourceFormat)
        val numMonth : Int = cal.get(Calendar.MONTH)
        if(numMonth > 4){

          cal.add(Calendar.YEAR, amount = 1)
          cal.get(Calendar.YEAR).toString

        }
        else {
          cal.get(Calendar.YEAR).toString
        }
      }

      )
    }
  )
  val targetTableSchema : TableConfig => StructType = tableConf => {

    val targetTable = s"${tableConf.target_database}.${tableConf.target_table}"
    getTargetType(tableConf.target_table_type) match {

      case HIVE => readHiveTable(targetTable).schema
      case KUDU => readKuduWithCondition(targetTable, where = false).schema
      case _ => StructType(Array.empty[StructField])

    }
  }
  val fiscalWeekOfYearUdf : UserDefinedFunction = udf (
    (value : String, sourceFormat : String) => {

      weekValidator(value)( value => {
        val cal : Calendar = getDateInstance(value, sourceFormat)
        val numMonth : Int = cal.get(Calendar.MONTH)
        if(numMonth < 4){

          cal.add(Calendar.MONTH, amount = 5)
          cal.get(Calendar.WEEK_OF_YEAR)

        }
        else {
          cal.add(Calendar.MONTH, amount = -5)
          cal.get(Calendar.WEEK_OF_YEAR)
        }
      }
      )

    }
  )
  val dateTimeFormatter : UserDefinedFunction = udf(
        (value : String, sourceFormat : String, targetFormat : String) => {
          dateValidator(value) ( value => {

            val parsedData = new SimpleDateFormat(sourceFormat).parse(value)
            val outputFormat = new SimpleDateFormat(targetFormat)
            outputFormat.format(parsedData)
          }

          )
        }
  )
  
  /* 
   * UDF function to format the different date values
   * */
  val getWeekOfYear : UserDefinedFunction = udf(
      (value : String, sourceFormat : String) => {
        weekValidator(value) (value => {
          val cal : Calendar = getDateInstance(value,sourceFormat)
          cal.get(Calendar.WEEK_OF_YEAR)

        })
      }
  )
  
  /* 
   * UDF function to format the different time values
   * 
   * */
  val getWeekOfMonth : UserDefinedFunction = udf(
        (value : String, sourceFormat : String) => {
          weekValidator(value) (value => {
            val cal : Calendar = getDateInstance(value,sourceFormat)
          cal.get(Calendar.WEEK_OF_MONTH)
          })

        }
  )
  val timeConversionUDf : UserDefinedFunction = udf(
        (value : Int, from : String, to_ : String) => {
          val inTime = Duration(value, from)
          import  TimeUnit._
          TimeUnit.valueOf(to_) match {

            case DAYS => inTime.toDays
            case HOURS => inTime.toHours
            case MINUTES => inTime.toMinutes
            case SECONDS => inTime.toSeconds
            case _ => inTime.toMinutes
          }
        }
  )
  val readLookUpData : TableConfig => DataFrame = lookUpConfig =>
    getReaderInstance(lookUpConfig.source_table_type).readData(lookUpConfig)._2
    val whereCondFunc : TableConfig => String = tableConfig =>
      Map(
      sourceSystem         -> tableConfig.source_system
      sourceDataRegionName -> tableConfig.sourcedata_regionname
    
  /*
   * Each transformation provides differentlogic based on sources
   * @param dataFrameSeq tuples of table config and data frame
   * @return
   *  */
    val configReader : String => TableConfig => Boolean => DataFrame =
      configTableName =>
        tableConfig => {
          val joinTableDf : DataFrame = readKuduWithCondition(
          configTableName,
          whereCondFunc(tableConfig)
          ).cache()

          throwError =>
            if(joinTableDf.isEmpty){
              import org.lera.etl.util.ETLException
              val errMessage : String =
                s"Missing entries in the $configTableName table for source system : $"
                if(throwError){
                  logger.error(errMessage)
                  throw new ETLException(errMessage)
                } else {
                  logger.warn(errMessage)
                }
            }
            joinTableDf
        }
    
    /* 
     * Read column mapping and update schema of dataframe
     * @param dataDF source dataframe
     * @param col array Mapping Array
     * @return DataFrame
     * 
     * */
      val getTableConfigFunc : String => TableConfig => DataFrame = table =>
        config => configReader(table)(config)(true)
    
    implicit class BaseImplicits(df : DataFrame) {
      
      def emptyValidator(columns : String*) : DataFrame = {
        columns.foldLeft(df)((df,columns) => {
          df.withColumn(
           column,
           when(trim(col(column)) === StringExpr.empty , value = null)
             .otherwise(col(column))
          )
        })
      }
      
      def zeroAndEmptyValidator(columns : String*) : DataFrame = {
          columns.foldLeft(df)((df,columns) => {
          df.withColumn(
           column,
           when(trim(col(column)) === StringExpr.empty || trim(col(column)) === 0 , value = null)
             .otherwise(col(column))
          )
        })
        
      }
      
      def IntToTimeStampConvertor(columns : Seq[String]): DataFrame = {
        
        columns.foldLeft(df)((df,column) => 
          df.withColumn(column,(col(column) / 1000).cast(TimeStampType))  
        )
      }
      
      def dateTimeConversion(
        timeFormatColumns : Seq[DateTimeConvert]    
      ): DataFrame = {
        
        timeFormatColumns.foldLeft(df)((df,tup3) => {
          val DateTimeConvert(
            sourceColumnName,
            targetColumnName,
            sourceFormat,
            targetFormat
          ) = tup3
          
          targetColumnName match {
            case Constants.weekOfMonth =>
              df.withColumn(
                targetColumnName,
                getWeekOfMonth(col(sourceColumnName), lit(sourceFormat))
              )
              
            case Constants.weekOfYear =>
              df.withColumn(
                targetColumnName,
                getWeekOfYear(col(sourceColumnName), lit(sourceFormat))
              )
            
            case Constants.fiscalYear =>
              df.withColumn(
                targetColumnName,
                fiscalYearUDF(col(sourceColumnName), lit(sourceFormat))
              )
              .replaceToNull(targetColumnName)
              
            case _ =>
              df.withColumn(
                targetColumnName,
                dateTimeFormatter(
                  col(sourceColumnName),
                  lit(sourceFormat),
                  lit(targetFormat)
                )
              
              )
              
          }
        }
            
        )
      }
      
      def replaceToNull(columnName : String): DataFrame = {
        df.withColumn(
          columnName,
          when(col(columnName) === 0 , value = null).otherwise(col(columnName))
        )
      }
      
      def concatValues(columnName : String, elements : Column*) : DataFrame = {
        df.withColumn(columnName, concat(elements: _*))
      }
      
      def timeConvert(timeFormatColumns : Seq[TimeConvert]) : DataFrame = {
        timeFormatColumns.foldLeft(df)((df,timeConvert) => {
          val TimeConvert(sourceColumn, targetColumn, sourceTime, targetTime) =
            timeConvert
            df.withColumn(
              targetColumn,
              timeConversionUDf(
              col(sourceColumn),
              lit(sourceTime.toString),
              lit(targetTime.toString)
              )
              
            )
        }
            
        )
        
      }
      
      // for Lynx inventory
      def DateFilter(columns : Seq[String]): DataFrame = {
        columns.foldLeft(df)(
        (df,column) => df.filter(col(column) >= lynxinventfilcond)    
        )
      }
      
    }
    
    implicit class ColumnImplicits(column : Column) {
      
      def lower: Column = {
        Functions.lower(column)
      }
      
      def length: Column = {
        Functions.length(column)
      }
      
      def trim: Column = {
        Functions.trim(column)
      }
    }
      val getDefaultTableConfigFunc : String => TableConfig => DataFrame = table =>
        config => configReader(table)(config)(false)
    
    implicit class ConditionImplicits(map : Map[String, String]){
      
      def toWhereCondition : String ={
        map
        .mapValues(value => s"'$value'")
        .map(keyValues => s"${keyValues._1}-${keyValues._2}")
        .mkString(" AND ")
      }
    }
    
    import scala.util.{Failure, Success, Try}
  private val logger : Logger = Logger.getLogger(this.getClass)
    
   def transform(
         dataFrameSeq : ParSeq[(TableConfig, DataFrame)]
   ) : ParSeq[(TableConfig, DataFrame)]
      ).toWhereCondition().ignoreCase.InSQL
      
    def renameDFColumnNames(dataDF : DataFrame, col_array : Array[(String, String)]) : DataFrame = {
      col_array.foldLeft(dataDF) ((df,tuple) =>
      df.withColumnRenamed(tuple._1, tuple._2)
      )
    }
        
    def defaultValuesForMissingColumns(df : DataFrame, schema : Seq[StructField]) : DataFrame = {
      val typeMapper : DataType => Any = {
        case IntegerType | LongType => 0
        case FloatType              => 0.0f
        case DoubleType             => 0.0
        case BooleanType            => false
        case _                      => StringExpr.empty

      }
      schema.foldLeft(df)((df,structType) =>{
        df.withColumn(structType.name, lit(typeMapper(structType.dataType)))

      })
    }
      
    // Check for the return type
    def configHandler[A <: ParSeq[TableConfig], B>: TableConfig, C](inSeq : A)(block : B => C) : C = {
      Try(block(inSeq.head)) match {
      case Success(configDataFrame) => configDataFrame
      case Failure(exception) =>
        inSeq.flatMap(tuple => {
          errorHandler(tuple)(throw exception)
        })
        throw exception
    }
    }
          
  
}