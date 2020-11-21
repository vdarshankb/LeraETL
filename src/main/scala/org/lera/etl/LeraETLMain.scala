package org.lera.etl

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.lera.connectionContextCreator

//import org.lera.ContextCreator.getProperty

import org.lera.connectionContextCreator.getProperty
import org.lera.etl.util.Constants.StringExpr
import org.lera.etl.util.KuduUtils
import org.lera.etl.util.Parser._
import org.lera.etl.util.utils._
//import org.lera.{ContextCreator, TableConfig}
import org.lera.TableConfig

import scala.collection.parallel.ParSeq
import scala.util.Try

object LeraETLMain {

  import scala.concurrent.Future

  private val logger: Logger = Logger.getLogger(LeraETLMain.getClass)

  def main(args: Array[String]): Unit = {

    logger.info(s"Checking command line arguments.")

    if ((args.length > 2 && null == args(2)) || (args.length == 2)) {
      logger.info(
        "Only two out of three arguments are provided. Load type parameter is taken from "
      )
    }

    if (args.length < 2) {
      logger.error(
        "Source System and Region, minimum two arguments are required."
      )
      throw new Exception("Incorrect arguments exception")
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    connectionContextCreator.createTables()

//    val (sourceSystem: String, tableConfigs: ParSeq[TableConfig]) = getTableConfig(args)
    val (sourceSystem: String, tableConfigs: ParSeq[TableConfig]) = (
      "test",
      ParSeq(
        TableConfig(
          "source_system",
          "sourcedata_regionname",
          "hive",
          "hive",
          "default",
          "source_table",
          null,
          "default",
          "target_table",
          "full",
          null,
          null
        )
      )
    )

    logger.info(s"Source System provided is $sourceSystem")
    logger.info(s"Arguments provided are ${args.seq.toString()}")
    logger.info(
      s"Processing for $sourceSystem and for the ${tableConfigs.toString()}"
    )


    startIngestionProcess(sourceSystem, tableConfigs)
//    ingestionMonitor(ingestionThread, tableConfigs, sourceSystem)
  }

  def ingestionMonitor(ingestionThread: Future[Unit],
                       config: ParSeq[TableConfig],
                       sourceSystem: String): Unit = {

    /*
    import java.sql.Timestamp


    val (notifyInterval: Int, maxRunTime: Int) = getControl(sourceSystem)

    println(notifyInterval, maxRunTime)

    logger.info(s"Notify Interval : $notifyInterval, maxRunTime : $maxRunTime")
    val startTime: Timestamp = now

    def timeDifference: Long = now.getTime - startTime.getTime
    def diffMinutes: Long =
      ((timeDifference / (60 * 60 * 1000)) * 60) + (timeDifference / (60 * 1000) % 60)

    val getTime: (Long, String) => String = (time, timeType) => {
      time match {
        case 0           => StringExpr.empty
        case x if x <= 1 => s"$time $timeType"
        case _           => s"$time ${timeType}s"
      }
    }

    def diffSeconds(timeDifference: Long = timeDifference): String =
      getTime(timeDifference / 1000 % 60, "second")

    def getMinutes(timeDifference: Long = timeDifference): String =
      getTime(timeDifference / (60 * 1000) % 60, "minute")

    def getHours(timeDifference: Long = timeDifference): String =
      getTime(timeDifference / (60 * 60 * 1000), "Hr")

    def runTime(diff: Long = timeDifference): String =
      s"${getHours(diff)} ${getMinutes(diff)} ${diffSeconds(diff)}".trim

    var timeTaken = notifyInterval

    while (!ingestionThread.isCompleted) {
      Thread.sleep(10000)
      if (diffMinutes >= timeTaken) {
        timeTaken = timeTaken + notifyInterval
        val message =
          s"Running more than ${s"${getHours()} ${getMinutes()}".trim}"
        sendEmailNotification(config, sourceSystem, message, runTime())
      }

      if (diffMinutes >= maxRunTime) {
        val updatedConf = config.map(
          conf =>
            conf.copy(
              conf.message + s"\n Job killed because job was running more than cut off time"
          )
        )

        sendEmailNotification(updatedConf,sourceSystem, message = "killed",runTime()
        )

        throw new ETLException(
          s"Job was running more than cut off time ($diffMinutes Minutes) so killing the job"
        )

      }
    }

    logger.info(s"Fatal time taken for execution: $diffMinutes minutes")
    val failedJobs: Array[TableRunInfo] = null

    // Close the spark session as job completed

    if (failedJobs.isEmpty) {
      logger.info(s"Data loading is completed for the source : $sourceSystem ")
      sendEmailNotification(
        config,
        sourceSystem,
        message = "completed",
        runTime()
      )
    } else {
      sendEmailNotification(
        config.map(conf => {
          val failedConf =
            failedJobs.filter(info => info.tableName == conf.target_table)
          if (failedConf.isEmpty) {
            conf
          } else {
            conf.copy(message = failedConf.head.errorMessage)
          }
        }),
        sourceSystem,
        message = "failed",
        runTime()
      )

      throw new ETLException(
        s"Data load failed for the table ${failedJobs.head.tableName} due to ${failedJobs.head.errorMessage}"
      )

    }

   */

  }

  def getControl(sourceSystem: String): (Int, Int) = {

    logger.info(s"Inside the getControl method.")

    val tableName =
      s"$etlAuditDatabase.${getProperty("spark.job_exec_control_table")}"
    val controlDf = KuduUtils.readKuduWithCondition(
      tableName,
      s"lower(source_system) = '${sourceSystem.toLowerCase()}'"
    )

    if (controlDf.count()==0) {
      val defaultNotificationTime: Int =
        Try(getProperty("spark.default_notification_interval_time"))
          .getOrElse("30")
          .toInt

      val defaultIntTime: Int =
        Try(getProperty("spark.default_max_run_time"))
          .getOrElse("120")
          .toInt

      (defaultNotificationTime, defaultIntTime)

    } else {
      controlDf
        .collect()
        .map(
          row => (row.getAs[Int]("notification_interval"), row.getAs[Int](""))
        )
        .head
    }
  }

  private def sendEmailNotification(config: ParSeq[TableConfig],
                                    sourceSystem: String,
                                    message: String,
                                    runTime: String): Unit = {

    /*   val recipients: String = getProperty("spark.notification_mail_list")
    val subject: String =
      s"${getProperty("spark.notification_mail_subject")} for $sourceSystem is "

    import EmailSMTPClient._

    val completeSubject: String = subject + message

 //Enable the commented code when you want to send email notifications

    EmailSMTPClient
      .sendMail(
        createHTMLBody(config.seq, runTime, message),
        completeSubject,
        recipients
      )
   */

  }

  def getTableConfig(args: Array[String]): (String, ParSeq[TableConfig]) = {

    import scala.util.{Failure, Success}

    val (sourceSystem, region) = (args(0), args(1))

    Try {

      val loadType: String = Try(args(2)).getOrElse(null)

      //The below links of code is to load the data from the table provided as the fourth argume.
      //In this project, this fourth arugument is not considered yet

      val tableNames: Array[String] =
        Try(args(3))
          .getOrElse(StringExpr.empty)
          .split(StringExpr.comma)
          .filterNot(_.isEmpty)

      logger.info(
        s"Inside getTableConfig method: sourceSystem = $sourceSystem, region = $region, loadType = $loadType, tableNames = ${tableNames.seq.toString()} "
      )

      (
        sourceSystem,
        getTableConfigs(sourceSystem, region, loadType, tableNames)
      )

    } match {
      case Success(value) => value
      case Failure(exception) =>
        val error = if (exception != null & exception.getMessage.nonEmpty) {
          exception.getMessage.split(StringExpr.line)(0)
        } else StringExpr.empty

        val emptyConf: TableConfig = getEmptyTableConfig
          .copy(
            source_system = sourceSystem,
            sourcedata_regionname = region,
            message = error
          )

        sendEmailNotification(Seq(emptyConf).par, sourceSystem, "failed", "")
        throw exception
    }

  }

  def startIngestionProcess(sourceSystem: String,
                            tableConfigs: ParSeq[TableConfig]): Unit = {

    import org.lera.etl.util.Enums.RunStatus._

    logger.info(
      s"Database and table information are parsed from the config table ${tableConfigs.seq.toString()}"
    )

    val rawData: ParSeq[(TableConfig, DataFrame)] = tableConfigs
      .map(tableConfig => {
        auditUpdate(tableConfig, RUNNING)
        tableConfig
      })
      .flatMap(tableConfig => {
        handler(tableConfig)(
          getReaderInstance(tableConfig.source_table_type).readData(tableConfig)
        )
      })

    //The below line is added by Darshan
    //  val rawData: ParSeq[(TableConfig, DataFrame)] = tableConfigs.flatMap((tableConfig => { handler(tableConfig)(getReaderInstance(tableConfig.source_table_type).readData(tableConfig))}))

    // Retain the below line for local testing
    // rawData.foreach(_._2.show(false))
    // Transformations logic applies on source data

    val transformedData: ParSeq[(TableConfig, DataFrame)] =
      getTransformers(sourceSystem)
        .foldLeft(rawData)((rawData, transIns) => {
          transIns.transform(rawData)
        })

    // Write transformed data into
    transformedData
      .map(values => {
        (values._1.target_table_type, values)
      })
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .foreach((values: (String, ParSeq[(TableConfig, DataFrame)])) => {
        getWriterInstance(values._1).write(values._2)
      })
  }

}
