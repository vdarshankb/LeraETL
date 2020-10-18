package org.lera.etl.util

object Enums {
  /*
   * Reader enums for creating instance of reader implementation
   *
   * Kudu 		 : Kudu based reader
   * Flat file : File based reader
   * */

  object Readers extends Enumeration {

    type readerType = Value
    val KUDU, EXCEL, CSV, JSON, TEXT, SQLKUDU, HIVE, RESTAPI = Value

    def apply(readerType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(readerType))

    def fromString(readerType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(readerType))
  }

  /*
   * Transformer enum for creating instance of Transformer implementation
   * */

  object Transformers extends Enumeration {

    type transformerType = Value

    /*
     * To be continued later based on the source systems
     * val towWorksContracts, ..... */

    def apply(transformerType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(transformerType))

    def fromString(transformerType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(transformerType))

  }

  /*
   * Writers enum for creating instance of writer implementation
   * */

  object Writers extends Enumeration {

    type writerType = Value
    val KUDU, HIVE, EXCEL = Value

    def apply(writerType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(writerType))

    def fromString(writerType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(writerType))

  }

  /*
   * Run status enum used for updating job progress in audit table
   * */

  object RunStatus extends Enumeration {

    type RunStatus = Value
    val RUNNING, SUCCESS, FAILED = Value

    def apply(status: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(status))

    def fromString(runStatus: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(runStatus))

  }

  /*
   * Loadertype enum used for updating loader type in table config tableConfigs
   * */

  object LoaderType extends Enumeration {

    type loadType = Value
    val INCREMENTAL, INCR, FULL = Value

    def apply(loadType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(loadType))

    def fromString(loadType: String): Option[Value] =
      values.find(value => value.toString.equalsIgnoreCase(loadType))

  }
}
