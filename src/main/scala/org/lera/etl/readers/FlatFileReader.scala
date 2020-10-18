package org.lera.etl.readers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.log4j.Logger
object FlatFileReader extends Reader{
  
  private val logger : Logger = Logger.getLogger(FlatFileReader.getClass)

override def readData(tableConf : TableConfig) : (TableConfig,DataFrame) = {
	if(!isFilePathExists(tableConf.file_path) && tableConf.source_table_type != csv) {
	throw new FileNotFoundException(
	s"File does not exist in : ${tableConf.file_path}"
)
}

import .Readers._
val df : DataFrame =
	Readers.fromString(tableConf.source_table_type).get match {
	case EXCEL =>
	  val targetSchema = readKuduWithCondition(
	  tableName = s"${tableConf.target_database}.${tableConf.target_table}",
	  where = false
).schema
readExcel(tableConf.file_path,targetSchema)

	case JSON => spark.read.json(tableConf.file_path)
	case CSV  => readCSV(tableConf.file_path, tableConf.source_system)
	case _    => spark.read.textFile(tableConf.file_path).toDF
}

(tableConf, df)
}
  
  /* 
   * Read excel file from local or hdfs location
   * 
   * @param fileName file location
   * @return
   * */
  
  def readExcel(fileName : String, schema : StructType): DataFrame = {
    
    val sheet = spark.conf.getOption(sheetName).getOrElse("Sheet1")
    
    logger.info(s"Reading excel file from : $fileName and sheet : $sheet")
    
    spark.read
     .format(source="com.crealytics.spark.excel")
     .schema(schema)
     .option("sheetName",sheet)
     .option("header",true.toString)
     .load(fileName)
     .filter(
       row =>
         !(
           row
             .mkString(StringExpr.empty)
             .replace(target="null",StringExpr.empty)
             .isEmpty && row.length > 0
         )
     )
     .trimColumnTrailingSpace
  }
  
  /* 
   * Get CSV file to dataframe
   * 
   * @param fileLocation file location
   * @param sourceSystem system
   * @return Number of files
   * */
  
  def readCSV(fileLocation : String, sourceSystem : String): DataFrame = {
    /* lazy val numOfFiles = getNumberOfFiles(fileLocation)
     * if(fileLocation.startsWith("file:///") || numOfFiles ==1)*/
    
    spark.read 
      .format(csvFileType.toLowerCase)
      .option("header",true)
      .option("quote","\"")
      .option("escape","\"")
      .option("timestampFormat","yyyy/MM/dd HH:mm:ss ZZ")
      .load(path=s"$fileLocation")
      
    /* else if(numOfFiles > 1) {
     * throw new IBPException(
     * s"Due to number of source files more than one at location $fileLocation"
     * )
     * } else {
     * throw new FileNotFoundException(
     * s"Number of files is 0. Please provide the CSV file for source system $sourceSystem"
     * )
     * }*/
  }
  
}