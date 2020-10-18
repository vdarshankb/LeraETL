package org.lera.etl.Writers

import org.apache.spark.sql.DataFrame

import scala.collection.parallel.immutable.ParSeq

/*
 * Writer for persisting dataset into target system
 * 
 * 
 *  */

trait Writer {
  
  /* 
   * Write datasets into target tables or file system
   * 
   * @param dataSet tuple or dataSet and table properties
   * 
   * */
  
  
  def write(dataSet : ParSeq[(TableConfig, DataFrame)])
  
  protected def getSourceSystemNames(df : DataFrame) : Array[String] = {
    df.selectExpr(sourceSystem)               : DataFrame
    .distinct                                 : Dataset[Row]
    .collect                                  : Array[Row]
    .map(_.get(0))                            : Array[Any]
    .filter(_ != null)                        : Array[Any]
    .map(_.toString().trim.toLowerCase)       : Array[String]
  } 
  
}