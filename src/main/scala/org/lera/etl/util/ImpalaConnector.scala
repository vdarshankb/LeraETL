package org.lera.etl.util
import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.lera.etl.util.utils.JDBC_URL_Generator

/*

  val connectionURL = ""

  val JDBCDriver = ""

  def executeQuery(query:String):Boolean={
    true
  }

  def buildQueryForInsert(tableName:String,tableName2:String,column:Array[String]) (isFullLoad:Boolean) = {
    ""
  }

  def buildDeleteStatement(tableName:String,cond:String="") ={
    ""
  }

}
*/

import org.lera.ContextCreator
import org.lera.etl.util.Constants._
import org.apache.log4j.Logger
import org.lera.etl.util._
import scala.util.Try
import org.apache.spark.sql.{DataFrame, SparkSession}

object ImpalaConnector extends ContextCreator{

  lazy val connection: Connection = DriverManager.getConnection(connectionURL)
  lazy val statement: Statement = connection.createStatement()

  val userName : String = getProperty(impalaUserName)
  val password : String = getProperty(impalaPassword)
  val JDBCDriver : String = getProperty(jdbcDriver)

  Class.forName(JDBCDriver).newInstance

  val connectionURL: String = JDBC_URL_Generator(getProperty(impalaURL), userName,password)

  private val logger: Logger = Logger.getLogger(ImpalaConnector.getClass)

  /*
  *Driver properties to initialize kudu table connection
  *
  */

  def getImpalaProperty: Properties = {
    val prob = new Properties
    prob.setProperty(driver,JDBCDriver)
    prob
  }

  /*
  *Generates delete table query to delete data from kudu table
  *
  *@param tableName impala table name which data need to be dropped
  *@return
  */

  def buildDeleteStatement(tableName: String, whereCond : String = StringExpr.empty) : String = {
    val cond =
      if(whereCond.isEmpty) StringExpr.empty
      else s"where $whereCond"

    s"DELETE $tableName $cond;"
  }

  /*
  *Build upsert query to upsert data from hive intermediate table into kudu tables
  *
  *@param srcHiveTableName hive intermediate table name
  *@param targetKuduTableName target kudu table name
  *@param columns column Names
  *
  */

  def buildQueryForInsert(
                           srcHiveTableName : String,
                           targetKuduTableName : String,
                           columns : Array[String]
                         )(isFullLoad: Boolean = true) : String = {
    val selectColumns =
      columns.map(column => s"'$column'.").mkString(StringExpr.comma)
    val insertType : String =
      if(isFullLoad) "INSERT"
      else "UPSERT"

    s"$insertType INTO TABLE $targetKuduTableName($selectColumns) SELECT $selectColumns FROM $srcHiveTableName;"
  }

  /*
  *Executes sql query in impala shell
  *
  *@param queries prepared query statement
  **/

  def executeQuery(queries : String*) = {
    logger.info(s"Executing query in Impala:: $queries")
    Try {
      queries.foreach(query => {
        println(s"Executing query $query")
        statement.execute(query)
      })
    } match {
      case Success(_) => true
      case Failure(exception) =>
        logger.error(
          s"Impala query execution has failed due to ${exception.getMessage}"
        )
        throw exception
    }
  }
}