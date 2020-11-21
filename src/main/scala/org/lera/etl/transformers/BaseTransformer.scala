package org.lera.etl.transformers

import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.lera.etl.util.Constants
import org.lera.etl.util.Constants.{StringExpr, _}
import org.lera.etl.util.DateTimeFormater._
import org.lera.etl.util.Enums.Writers._
import org.lera.etl.util.KuduUtils._
import org.lera.etl.util.Parser._
import org.lera.etl.util.utils._

import org.lera.{connectionContextCreator, DateTimeConvert, TableConfig, TimeConvert}

/*
import org.lera.ContextCreator
*/

import scala.collection.parallel.ParSeq
import scala.concurrent.duration.Duration

trait BaseTransformer {

  private val logger: Logger = Logger.getLogger(this.getClass)
  logger.info("Inside the trait BaseTransformer")

  val fiscalYearUDF: UserDefinedFunction = udf(
    (value: String, sourceFormat: String) => {

      dateValidator(value)(value => {

        val cal: Calendar = getDateInstance(value, sourceFormat)
        val numMonth: Int = cal.get(Calendar.MONTH)
        if (numMonth > 4) {

          cal.add(Calendar.YEAR, 1)
          cal.get(Calendar.YEAR).toString

        } else {
          cal.get(Calendar.YEAR).toString
        }
      })
    }
  )

  val targetTableSchema: TableConfig => StructType = tableConf => {

    val targetTable = s"${tableConf.target_database}.${tableConf.target_table}"

    logger.info(s"Reading schema for target table: $targetTable and its of type: ${tableConf.target_table_type}")

    getTargetType(tableConf.target_table_type) match {
      case HIVE => readHiveTable(targetTable).schema
      case KUDU => readKuduWithCondition(targetTable, where = false).schema
      case _    => StructType(Array.empty[StructField])

    }
  }

  val fiscalWeekOfYearUdf: UserDefinedFunction = udf(
    (value: String, sourceFormat: String) => {

      weekValidator(value)(value => {
        val cal: Calendar = getDateInstance(value, sourceFormat)
        val numMonth: Int = cal.get(Calendar.MONTH)
        if (numMonth < 4) {

          cal.add(Calendar.MONTH, 5)
          cal.get(Calendar.WEEK_OF_YEAR)

        } else {
          cal.add(Calendar.MONTH, -5)
          cal.get(Calendar.WEEK_OF_YEAR)
        }
      })

    }
  )

  val dateTimeFormatter: UserDefinedFunction = udf(
    (value: String, sourceFormat: String, targetFormat: String) => {
      dateValidator(value)(value => {

        val parsedData = new SimpleDateFormat(sourceFormat).parse(value)
        val outputFormat = new SimpleDateFormat(targetFormat)
        outputFormat.format(parsedData)
      })
    }
  )

  /*
   * UDF function to format the different date values
   * */
  val getWeekOfYear: UserDefinedFunction = udf(
    (value: String, sourceFormat: String) => {
      weekValidator(value)(value => {
        val cal: Calendar = getDateInstance(value, sourceFormat)
        cal.get(Calendar.WEEK_OF_YEAR)

      })
    }
  )

  /*
   * UDF function to format the different time values
   *
   * */
  val getWeekOfMonth: UserDefinedFunction = udf(
    (value: String, sourceFormat: String) => {
      weekValidator(value)(value => {
        val cal: Calendar = getDateInstance(value, sourceFormat)
        cal.get(Calendar.WEEK_OF_MONTH)
      })

    }
  )

  val timeConversionUDf: UserDefinedFunction = udf(
    (value: Int, from: String, to_ : String) => {
      val inTime = Duration(value, from)
      import TimeUnit._
      TimeUnit.valueOf(to_) match {

        case DAYS    => inTime.toDays
        case HOURS   => inTime.toHours
        case MINUTES => inTime.toMinutes
        case SECONDS => inTime.toSeconds
        case _       => inTime.toMinutes
      }
    }
  )

  val readLookUpData: TableConfig => DataFrame = lookUpConfig =>
    getReaderInstance(lookUpConfig.source_table_type).readData(lookUpConfig)._2

  //Prepare the where condition based on the source_system and sourcedata_regionname
  val whereCondFunc: TableConfig => String = tableConfig =>
    Map(
      sourceSystem -> tableConfig.source_system,
      sourceDataRegionName -> tableConfig.sourcedata_regionname
    ).toWhereCondition


  /*
   * Each transformation provides different logic based on sources
   * @param dataFrameSeq tuples of table config and data frame
   * @return
   *  */
  val configReaderFromHiveTable: String => TableConfig => Boolean => DataFrame =
    configTableName =>
      tableConfig => {
        val joinTableDf: DataFrame = {
          readHiveWithCondition(configTableName, whereCondFunc(tableConfig))
            .cache()
        }

        throwError =>
          if (joinTableDf.count()==0) {
            import org.lera.etl.util.ETLException
            val errMessage: String =
              s"Missing entries in the $configTableName table for source system : $sourceSystem"
            if (throwError) {
              logger.error(errMessage)
              throw new ETLException(errMessage)
            } else {
              logger.warn(errMessage)
            }
          }
          joinTableDf
      }

  /*
   * Each transformation provides differentlogic based on sources
   * @param dataFrameSeq tuples of table config and data frame
   * @return
   *  */
  val configReader: String => TableConfig => Boolean => DataFrame =
    configTableName =>
      tableConfig => {
        val joinTableDf: DataFrame = {
          logger.info(s"Before calling the readHiveWithCondition in the BaseTransformer.configReader method. source_config_table_type = $source_config_table_type")
          if(source_config_table_type == "Hive") (
            readHiveWithCondition(configTableName, whereCondFunc(tableConfig)).cache()
            )
          else readKuduWithCondition(configTableName, whereCondFunc(tableConfig)).cache()
        }

        throwError =>
          if (joinTableDf.count()==0) {
            import org.lera.etl.util.ETLException
            val errMessage: String =
              s"Missing entries in the $configTableName table for source system : $sourceSystem"
            if (throwError) {
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
  val getTableConfigFunc: String => TableConfig => DataFrame = table =>
    config => configReader(table)(config)(true)

  implicit class BaseImplicits(df: DataFrame) {

    def emptyValidator(columns: String*): DataFrame = {
      columns.foldLeft(df)((df, column) => {
        df.withColumn(
          column,
          when(trim(col(column)) === StringExpr.empty, value = null)
            .otherwise(col(column))
        )
      })
    }

    def zeroAndEmptyValidator(columns: String*): DataFrame = {
      columns.foldLeft(df)((df, column) => {
        df.withColumn(
          column,
          when(
            trim(col(column)) === StringExpr.empty || trim(col(column)) === 0,
            value = null
          ).otherwise(col(column))
        )
      })

    }

    def IntToTimeStampConvertor(columns: Seq[String]): DataFrame = {

      columns.foldLeft(df)(
        (df, column) =>
          df.withColumn(column, (col(column) / 1000).cast(TimestampType))
      )
    }

    def dateTimeConversion(
      timeFormatColumns: Seq[DateTimeConvert]
    ): DataFrame = {

      timeFormatColumns.foldLeft(df)((df, tup3) => {
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
      })
    }

    def replaceToNull(columnName: String): DataFrame = {
      df.withColumn(
        columnName,
        when(col(columnName) === 0, value = null).otherwise(col(columnName))
      )
    }

    def concatValues(columnName: String, elements: Column*): DataFrame = {
      df.withColumn(columnName, concat(elements: _*))
    }

    def timeConvert(timeFormatColumns: Seq[TimeConvert]): DataFrame = {
      timeFormatColumns.foldLeft(df)((df, timeConvert) => {
        val TimeConvert(sourceColumn, targetColumn, sourceTime, targetTime) =
          timeConvert
        df.withColumn(
          targetColumn,
          timeConversionUDf(col(sourceColumn), lit(sourceTime), lit(targetTime))
        )
      })
    }
  }

  implicit class ColumnImplicits(column: Column) {

    def lower: Column = {
      lower(column)
    }

    def length: Column = {
      length(column)
    }

    def trim: Column = {
      trim(column)
    }
  }

  val getDefaultTableConfigFunc: String => TableConfig => DataFrame = table => config => configReader(table)(config)(false)

  logger.info(s"getDefaultTableConfigFunc value is: $getDefaultTableConfigFunc")

  implicit class ConditionImplicits(map: Map[String, String]) {

    def toWhereCondition: String = {
      map
        .mapValues(value => s"'$value'")
        .map(keyValues => s"${keyValues._1}=${keyValues._2}")
        .mkString(" AND ")
    }
  }

  import scala.util.{Failure, Success, Try}

  def transform(
    dataFrameSeq: ParSeq[(TableConfig, DataFrame)]
  ): ParSeq[(TableConfig, DataFrame)]

  def renameDFColumnNames(dataDF: DataFrame,
                          col_array: Array[(String, String)]): DataFrame = {
    col_array.foldLeft(dataDF)(
      (df, tuple) => df.withColumnRenamed(tuple._1, tuple._2)
    )
  }

  def defaultValuesForMissingColumns(df: DataFrame,
                                     schema: Seq[StructField]): DataFrame = {
    val typeMapper: DataType => Any = {
      case IntegerType | LongType => 0
      case FloatType              => 0.0f
      case DoubleType             => 0.0
      case BooleanType            => false
      case _                      => StringExpr.empty

    }
    schema.foldLeft(df)((df, structType) => {
      df.withColumn(structType.name, lit(typeMapper(structType.dataType)))

    })
  }

  // Check for the return type
  def configHandler[A <: ParSeq[TableConfig], B >: TableConfig, C](
    inSeq: A
  )(block: B => C): C = {
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
