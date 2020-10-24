package org.lera.etl.transformers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
trait JoinBaseTransformer extends BaseTransformer {

  val getDropColumns: DataFrame => DataFrame => Seq[Column] = sourceDf =>
    lookupDf => {
      lookupDf.columns
        .map(_.toLowerCase)
        .filter(columnName => sourceDf.columns.contains(columnName.toLowerCase))
        .map(name => lookupDf.col(name))
        .toSeq
  }

  val nullReplaceDf: DataFrame => Seq[String] => DataFrame = df =>
    columns => {
      columns.foldLeft(df)(
        (df, column) =>
          df.withColumn(
            column,
            when(col(column).trim.lower <=> "null", value = null)
              .otherwise(col(column))
        )
      )
  }

  val conditionCreator
    : DataFrame => DataFrame => Map[String, String] => Column = sourceDf =>
    lookUpDf => {
      _.map(tuple => {
        import org.apache.spark.sql.functions.lower
        lower(sourceDf.col(tuple._1)) <=> lower(lookUpDf.col(tuple._2))
      }).reduce((column1, column2) => {
        column1 && column2
      })
  }
}
