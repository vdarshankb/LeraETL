package org.lera.etl.readers

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonReader extends Reader with PipelineBase {

  private val logger: Logger = Logger.getLogger(ExcelReadr.getClass)

  /*
   *
   * Read data from source
   * @Param properties file
   * @return
   */

  def main(args: Array[String]): Unit = {

    val jsonDF = readData(properties, spark)

    jsonDF.show()
    jsonDF.printSchema()
    println("Records in dataframe : " + jsonDF.count())
  }

  override def readData(properties: Properties,
                        spark: SparkSession): DataFrame = {

    val filePath = properties.getProperty("filepath-json")
    logger.info(s"Reading data from JSON file $filePath")

    val multiLine = properties.getProperty("multiline-json")
    val dataFrame: DataFrame =
      properties.getProperty("load_type_json").toLowerCase match {

        case "incremental" | "incr" =>
          val incrementalCondition: String = getIncrementalLoadFilterCondition()
          logger.info(s"Incremental condition is $incrementalCondition")
          val sourceDataFrame: DataFrame =
            readJsonFile(spark, filePath, multiLine)
          if (incrementalCondition.nonEmpty)
            sourceDataFrame.where(incrementalCondition)
          else
            sourceDataFrame
        case "fullloadtype" => readJsonFile(spark, filePath, multiLine)

      }
    dataFrame
  }

  def getIncrementalLoadFilterCondition(): String = {
    return "Empty function.. to be continued later"
  }

  def readJsonFile(spark: SparkSession,
                   filePath: String,
                   multiLine: String): DataFrame = {
    var df_json = spark.read
      .option("multiLine", multiLine)
      .json(filePath)

    var nested_column_count = 1
    while (nested_column_count != 0) {

      var nested_column_count_temp = 0

      //Iterating each column again to check if any nested data still exists

      for (column_name <- df_json.schema.fieldNames) {

        //Checking type of column is array / struct

        if (df_json.schema(column_name).dataType.isInstanceOf[ArrayType]) {
          nested_column_count_temp += 1
        } else if (df_json
                     .schema(column_name)
                     .dataType
                     .isInstanceOf[StructType]) {
          nested_column_count_temp += 1
        }
      }

      if (nested_column_count_temp != 0) {
        df_json = expand_nested_column(df_json)
        df_json.show()
        df_json.printSchema()
      }
      nested_column_count = nested_column_count_temp

    }

    return df_json
  }

  def expand_nested_column(nested_df: DataFrame): DataFrame = {

    var json_data_df = nested_df
    var select_clause_list = List.empty[String]

    /* Iterating on columns to expand the nested structure*/
    for (column_name <- json_data_df.schema.fieldNames) {

      //Expanding for array type

      if (json_data_df.schema(column_name).dataType.isInstanceOf[ArrayType]) {

        json_data_df = json_data_df.withColumn(
          column_name,
          explode(json_data_df(column_name))
        )
        select_clause_list :+= column_name
      }

      //Expanding for Struct type
      else if (json_data_df
                 .schema(column_name)
                 .dataType
                 .isInstanceOf[StructType]) {

        for (field <- json_data_df
               .schema(column_name)
               .dataType
               .asInstanceOf[StructType]
               .fields) {
          select_clause_list :+= column_name + "." + field.name
        }
      } else {
        select_clause_list :+= column_name
      }
    }

    json_data_df.show()
    val columnNames =
      select_clause_list.map(name => col(name).alias(name.replace(".", "_")))
    val json_data_df_new = json_data_df.select(columnNames: _*)

    json_data_df_new

  }

}
