package org.lera.etl

import org.apache.spark.sql.SparkSession

object Test {


  def main(args: Array[String]): Unit = {


    val ss: SparkSession = SparkSession.builder().enableHiveSupport().appName("test")
      .master("local").getOrCreate()


    ss.table("default.target_table").show()






  }

}
