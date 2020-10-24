package org.lera.etl.util

object Constants {
val stringNULL="null"
  val fullLoadType: String = "full"
  val incremental: String = "incremental"
  val incr: String = "incr"

  val csv: String = "csv"
  val sourceSystem: String = "source_system"
  val sourceDataRegionName: String = "sourcedata_regionName"

  val partitionColumn: String = ""
  val numOfPartitions: String = ""

  val sourceDB: String = "source_database"
  val targetDB: String = "target_database"

  val configDB: String = "spark.lera_config_database"
  val auditDB: String = "spark.lera_audit_database"
  val configTableName: String = "spark.lera_config"
  val leraColumnMappingTableName: String = "spark.lera_column_mapping_table"
  val leraAuditTbl: String = "spark.lera_audit"
  val leraDefaultValuesTable: String = "spark.lera_default_values_table"
  val leraJoinTable: String = "spark.lera_join_table"
  val leraLookupTable: String = "spark.lera_generic_lookup_table"
  val leraConditionTable: String = "spark.lera_condition_table"
  val leraDeleteTable: String = "spark.lera_delete_table"
  val leraFilterTable: String = "spark.lera_filer_table"
  val skipMissingTargetColumn: String = "spark.skip_missing_target_column_source_"
  val skipMissingSourceColumn: String =  "spark.skip_missing_source_column_source_"

  val concatChar: String = "_"
  val sourceTable: String = "source_table"
  val targetTable: String  = "target_table"
  val filePath: String = "file_path"
  val sourceType: String = "source_type"
  val targetType: String =  "target_type"
  val loadType: String = "load_type"
  val sourceIncrementColumn: String = "source_increment_column"
  val targetIncrementColumn: String = "target_increment_column"
  val sourceColumn: String = "source_column"
  val targetColumn: String = "target_column"
  val defaultValue: String = "default_value"
  val sheetName: String = "spark.excel_sheet_name"

  //join table columns
  val lookupDB: String = ""
  val lookupTable: String = ""
  val lookupColumn: String = ""
  val joinType: String = ""
  val joinOrder: String = ""
  val lookupOrder: String = ""
  val lookupTableType: String = ""
  val sqlAND: String = " AND "
  val sqlOR: String = " OR "

  val partitionTableName: String = ""
  val database_name: String = ""
  val tableName: String = ""
  val partitionColumnName: String = ""
  val partitionColumnType: String = ""
  val numberOfPartitions: String = ""
  val lowerBound: String = ""
  val upperBound: String = ""


  val aggMaxColumn: String = "agg_max_value"

  val userName: String = "<username>"
  val password: String = "<password>"
  val hdfsFileLocationPrefix: String = ""

  val sparkAppName: String = "spark.app.name"
  val loggerLevel: String = "spark.log_level"
  val impalaURL: String = "spark.impalaConnectionURL"
  val jdbcDriver: String = "spark.impalaJDBCDriver"
  val impalaUserName: String = "spark.impala_username"
  val impalaPassword: String = "spark.impala_password"
  val driver: String = "driver"
  val kuduType: String = "Kudu"
  val hiveType: String = "Hive"
  val stagingIdentifier: String = "_cdp"
  val typeCast: String = "TypeCast"
  val status: String = "status"
  val inActive: String = "inactive"
  val rowNumber: String = "row_num"

  object StringExpr {

    def empty: String = ""
    def line: String = "\n"
    def plus : String = "\\+"
    def startBrace: String = "("
    def endBrace: String = ")"
    def space: String = " "
    def pipe: String = "|"
    def comma: String = ","
    def quote: String = "'"
    def equal: String = "="
    def notEqual: String = "!="
    def ques: String = "?"
    def hyphen: String = "-"
    def dot: String = "\\."
    def slash: String = "\" "
    def underScore: String = "_"
  }

  implicit
  class StringImplicits(consValue: String) {
    def ignoreCaseInSQL: String = {
      val equalReplacer: String = "))=LOWER(TRIM("
      val notEqualReplacer: String = "))!=LOWER(TRIM("
      val sqlAndReplacer: String = ")) AND LOWER(TRIM("
      val sqlOrReplacer: String = ")) OR LOWER(TRIM("
      s"LOWER(TRIM($consValue))".replace(StringExpr.equal, equalReplacer)
        .replace(StringExpr.notEqual, notEqualReplacer)
        .replace(sqlAND, sqlAndReplacer).replace(sqlOR, sqlOrReplacer)
    }
  }

}
