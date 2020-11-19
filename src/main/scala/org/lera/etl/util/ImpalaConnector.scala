package org.lera.etl.util
import java.sql.{Connection, Statement}
import java.util.Properties

import org.lera.etl.util.utils.JDBC_URL_Generator

import scala.util.{Failure, Success}
import org.apache.log4j.Logger
import org.lera.connectionContextCreator.{getProperty, spark}
import org.lera.etl.util.Constants._

import scala.util.Try

object jdbcConnector {

  import java.sql.DriverManager

  val userName : String = getProperty(jdbcUserName)
  val password : String = getProperty(jdbcPassword)
  val JDBCdriver : String = getProperty(jdbcDriver)

  Class.forName(JDBCdriver)

  val connectionURL: String = JDBC_URL_Generator(getProperty(jdbcURL), userName, password)

  lazy val connection: Connection = DriverManager.getConnection(connectionURL)
  lazy val statement: Statement = connection.createStatement()

  private val logger: Logger = Logger.getLogger(jdbcConnector.getClass)

  /* Driver properties to initialise jdbc table connection
  *
  * @return
   */

  def getJdbcProperty: Properties = {
    val prob = new Properties
    prob.setProperty(driver,JDBCdriver)
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
 *@param targetTableName target kudu table name
 *@param columns column Names
 *
 */

  def buildQueryForInsert(
                           sourceTableName : String,
                           targetTableName : String,
                           columns : Array[String]
                         )(isFullLoad: Boolean = true) : String = {
    val selectColumns =
      columns.map(column => s"'$column'.").mkString(StringExpr.comma)
    val insertType : String =
      if(isFullLoad) "INSERT"
      else "UPSERT"

    s"$insertType INTO TABLE $targetTableName($selectColumns) SELECT $selectColumns FROM $sourceTableName;"
  }

  /*
  *Executes sql query in impala shell
  *
  *@param queries prepared query statement
  **/

  def executeQueryUsingImpala(queries : String*): Boolean = {
    logger.info(s"Executing queries: $queries")
    Try {
      queries.foreach(query => {
        logger.info(s"Executing query $query")
        statement.execute(query)
      })
    } match {
      case Success(_) => true
      case Failure(exception) =>
        logger.error(s"Query execution failed due to ${exception.getMessage}")
        throw exception
    }
  }

}