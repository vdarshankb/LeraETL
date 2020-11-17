package org.lera.etl.util

import org.apache.log4j.Logger
import org.apache.spark.sql.{Encoder, Encoders}

import org.lera.etl.Writers._
import org.lera.etl.readers._
import org.lera.etl.transformers._
import org.lera.etl.util.Constants.{StringExpr, StringImplicits, incremental}
import org.lera.etl.util.Enums.{LoaderType, Transformers, Writers}
import org.lera.etl.util.utils._

import scala.collection.parallel.{MIN_FOR_COPY, ParSeq}

import org.lera.connectionContextCreator.getSparkSession
import org.lera.{connectionContextCreator, TableConfig}

//Added case class for loading partition table
                       
case class PartitionTableConfig(source_system:String,
                                sourcedata_regionname:String,
                                database_name:String,
                                table_name:String,
                                partition_column:String,
                                partition_column_type:String,
                                number_of_partitions:Int,
                                lower_bound:Int,
                                upper_bound:Int)
                                
/* 
 * Parser used to parse the table config properties from kudu meta data table
 * */

object Parser {

  implicit val dailyInvEncoder : Encoder[TableConfig] =
    Encoders.product[TableConfig]
  
  private val logger : Logger = Logger.getLogger(Parser.getClass)
  
  def getTransformers(sourceSystem:String): Seq[BaseTransformer] = {
    
    import Enums.Transformers._
    
    val genericTransformers : Seq[String] = Seq(ColumnMap, DefaultValue, TypeCast).map(_.toString)

    //List all the transformers required for each source system
   //The transformers can be provided in properties file as comma separated
  // make changes in below blocks of code

  val transformers : Seq[String] = getSparkSession.conf
    .getOption(s"spark.${sourceSystem.toLowerCase}_transformers")
    .getOrElse({
      logger.warn(
      s"Transformers type not provided in param : spark.${sourceSystem}_transformers"    
      )
      
      Transformers.fromString(transformerType=sourceSystem.toLowerCase) match {
        
        case Some(_) =>
          logger.info(
          s"Source based transformer type found : ${sourceSystem.toLowerCase}"    
          )
          insert(
           genericTransformers,
          genericTransformers.length -1,
           sourceSystem
          ).mkString(StringExpr.comma)
          
        case _ =>
          logger.info(
          s"Generic transformers added for execution are: $genericTransformers"
          )
          
          genericTransformers.mkString(StringExpr.comma)
      }
    })
    .split(StringExpr.comma)
    
    logger.info(s"Transformers added for execution : $transformers")
    transformers.map(getTransformerInstances) 
    
  }
  
  /* 
   * Get list of transformer tableConfigs
   * 
   * @param transformerType transformer Enum
   * @return
   * */
  
  def getTransformerInstances(transformerType:String): BaseTransformer = {
    
    val transType: Enums.Transformers.Value = Transformers
      .fromString(transformerType)
      .getOrElse(
        throw new Exception(s"Unknown transformer type : $transformerType")
      )

    logger.info(s"Transtype Enumeration value is: $transType")

    import Enums.Transformers._
      
    transType match {
      case ColumnMap           => ColumnMappingTransformer
      case DefaultValue        => DefaultValueSetTransformer
      case Joiner              => JoinTransformer
      case Filter              => FilterTransformer
      case Delete              => DeleteTransformer
      case TypeCast            => TypeCastTransformer
    }
  }
  
  /* 
   * Based on the source system the table config tableConfigs created
   * 
   * @param sourceSystem source system
   * @param tableName list of table names
   * @return 
   * */
  
  def getTableConfigs(sourceSystem:String,
                      region : String,
                      loadTypeArg: String,
                      tableName : Seq[String]): ParSeq[TableConfig] = {

    logger.info(s"Inside the getTableConfigs method.")
    logger.info(s"Transformations and data ingestion for all the tables under the system: $sourceSystem")
    
    val filterCondition : String = s"${Constants.sourceSystem} = '$sourceSystem' AND ${Constants.sourceDataRegionName}='$region'".ignoreCaseInSQL

    logger.info(s"Filter condition is set and now it will invoke ConfigReader.configLoader($filterCondition)")

    val tableConfigs: ParSeq[TableConfig] = {

        val configs: ParSeq[TableConfig] = ConfigReader.configLoader(filterCondition).par

      logger.info(s"After configLoader, return value of configs is ${configs.toString()}")
      logger.info(s"Table Name is empty or not: ${tableName.isEmpty}")

        if(tableName.isEmpty) configs
        else configs.filter(conf => tableName.contains(conf.target_table))

    }

    logger.info(s"Content of table Configs after the configLoader is: $tableConfigs")

    if(tableConfigs.isEmpty){
      throw new Exception(
      s"Related entries are not available for $sourceSystem in the config table"    
      )
    }

    logger.info(s"Load Type Argument is: $loadTypeArg")

    if(null != loadTypeArg){
      val loaderType : String =
        LoaderType.fromString(loadType = loadTypeArg.toLowerCase) match {
        case Some(outValue) => outValue.toString.toLowerCase()
        case _ =>
          val loaderUpdate : ParSeq[TableConfig] =
            tableConfigs.map(ins => ins.copy(load_type = loadTypeArg))
          val updatedTableConf : ParSeq[TableConfig] = loaderUpdate.map(
          msg => 
            msg.copy(
            message = s"Invalid load type provided :: $loadTypeArg"
            )
          )
          import Enums.RunStatus.FAILED
          auditUpdate(updatedTableConf.head, FAILED)
          throw new Exception(s"Invalid load type provided $loadTypeArg")
      }

      logger.info(s"$loaderType load type is in progress for source system: ${tableConfigs.head.source_system}")
      tableConfigs.map(ins => ins.copy(load_type = loaderType))
    } else tableConfigs
  }
  
  /* 
   * Get list of reader table configs
   * 
   * @param readType enum reader type
   * @return
   * */


  import Enums.Readers

  def getReaderInstance(readType:String): Reader = {

    logger.info(s"Read Type is: $readType")

    val readerType : Enums.Readers.Value = Readers
      .fromString(readerType = readType)
      .getOrElse(throw new Exception(s"Unknown reader type : $readType"))
      
    import Enums.Readers._
    readerType match {
      case KUDU                      => KuduReader
      case EXCEL | CSV | JSON | TEXT => FlatFileReader
      case SQLKUDU                   => SQLKuduReader
      case HIVE                      => HiveReader
      case RESTAPI                   => APIDataReader
    }
  }
  
  /* 
   * Get list of writer tableConfigs
   * @param writeType writer type
   * @return
   * */
  
  def getWriterInstance(writeType:String): Writer = {

    logger.info(s"Writer Type is: $writeType")

    val writerType: Enums.Writers.Value = Writers
      .fromString(writeType)
      .getOrElse(throw new Exception(s"Unknown writer type : $writeType"))
      
    import Enums.Writers._
    writerType match {
      case KUDU => KuduWriter
      case HIVE => HiveWriter
    }
  }
  
}