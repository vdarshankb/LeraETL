package org.lera.etl.util

import org.lera.ContextCreator

object ImpalaConnector extends ContextCreator{

  val connectionURL=""
  val JDBCDriver=""
  def executeQuery(query:String):Boolean={
    true
  }

  def buildQueryForInsert(tableName:String,tableName2:String,column:Array[String])(isFullLoad:Boolean)={
    ""

  }

  def buildDeleteStatement(tableName:String,cond:String="")={
    ""
  }

}
