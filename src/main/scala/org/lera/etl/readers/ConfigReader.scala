package org.lera.etl.readers

import java.util.Properties

import org.apache.log4j.Logger
import scala.reflect.runtime.universe.MethodSymbol
import org.lera.etl.transformers.BaseTransformer
import org.lera.etl.util.Constants._
import org.lera.etl.util.KuduUtils.{readHiveWithCondition, readKuduWithCondition}
import org.lera.etl.util.utils.{configDatabase, configTable}
import org.lera.TableConfig
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

object ConfigReader {

  // Commented by Darshan
  // lazy val ETL_ConfigTable: String = s"$configDatabase.$configTable"
  lazy val ETL_ConfigTable: String = s"$configDatabase.$configTable"
  println(s"Config table is: $ETL_ConfigTable")

  private val logger: Logger = Logger.getLogger(this.getClass)

  private val propertyCheck: Map[String, String] => (String, Boolean) => String =
    propertyHolder =>
      (propertyName, isRequired) => {

        val value: String = propertyHolder(propertyName)

        if (isRequired && (null == value || value.isEmpty)) {
          import org.lera.etl.util.ETLException
          val errorDesc: String = s"Required property '$propertyName' is missing from $ETL_ConfigTable"
          logger.error(errorDesc)
          throw new ETLException(errorDesc)
        } else {
          value
        }
      }

  def configLoader(filterCondition: String): Seq[TableConfig] =
  {

    logger.info(s"Inside the ConfigReader.configLoader method and filterCondition is $filterCondition")

    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.Encoders

   /* Commented as we dont have Kudu but now calling thew below readHiveWithCondition method
      val configs: Dataset[Config] =
      readKuduWithCondition(ETL_ConfigTable, filterCondition).as[Config](Encoders.product[Config])
   */

    logger.info(s"Invoking readHiveWithCondition method with Table = $ETL_ConfigTable and condition = $filterCondition")

    val configs: Dataset[Config] =
      readHiveWithCondition(ETL_ConfigTable, filterCondition).as[Config](Encoders.product[Config])

    logger.info(s"After triggering readHiveWithCondition method, the contents of the configs dataset is ${configs.collect().seq.toString()}")
    logger.info("configs.show is as below")
    configs.show(20)

    logger.info("Calling configCreator with and above displayed configs dataset data")

    val localTableConfig: Seq[TableConfig] = configCreator(configs.collect())

    logger.info(s"Return value from the configCreator method: ${localTableConfig.toString()}")

    localTableConfig
  }

  def configCreator(configs: Seq[Config]): Seq[TableConfig] = {

    logger.info("Inside the ConfigReader.configCreator method. Creating TableConfig with properties")

    val tableConf: Seq[Map[String, String]] = configs
      .groupBy(_.table_order)
      .values
      .map(_.map(conf => {
        (conf.property_name.trim, conf.property_value.trim)
      }).toMap).toSeq

    //Get the column names or the attributes from the TableConfig case class
    val columns:List[String] = classAccessors[TableConfig]

    logger.info(s"Contents of the tableConf object: ${tableConf.toString()}")
    logger.info(s"Values in the columns object: ${columns.toString()}")

    val tempTableConf: Seq[TableConfig] =  tableConf.map(
          conf => {
          val properties: Map[String, String] =

            columns.map(column => (column, conf.getOrElse(column, null))).toMap

            println("Printing both columns and conf contents" , columns, properties)

          val requiredProperty: String => String = propertyCheck(properties)(_, true)
          val optionalProperty: String => String = propertyCheck(properties)(_, false)

          TableConfig(
            requiredProperty(sourceSystem),
            requiredProperty(sourceDataRegionName),
            requiredProperty(sourceType),
            requiredProperty(targetType),
            requiredProperty(sourceDB),
            requiredProperty(sourceTable),
            requiredProperty(filePath),
            requiredProperty(targetDB),
            requiredProperty(targetTable),
            requiredProperty(loadType),
            optionalProperty(sourceIncrementColumn),
            optionalProperty(targetIncrementColumn)
          )
        }
        )

    logger.info(s"Table Config source database: ${tempTableConf.head.source_database}")
    logger.info(s"Table Config target database: ${tempTableConf.head.target_database}")
    logger.info(s"Table Config source_table: ${tempTableConf.head.source_table}")
    logger.info(s"Table Config Source System Contents: ${tempTableConf.head.source_system}")

    tempTableConf

  }

  def classAccessors[T: TypeTag]: List[String] =
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
    }.toList.reverse

}

case class Config(source_system: String,
                  sourcedata_regionname: String,
                  property_name: String,
                  property_value: String,
                  table_order: Int
                 )