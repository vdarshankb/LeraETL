package org.lera.etl.util

import java.text.SimpleDateFormat
import java.util.Calendar

object DateTimeFormater {

  val getDateInstance: (String, String) => Calendar = (value, sourceFormat) => {
    val parsedData = new SimpleDateFormat(sourceFormat).parse(value)
    val cal = Calendar.getInstance
    cal.setTime(parsedData)
    cal
  }

  val nullChecker: String => Boolean = value =>
    value == null || value.isEmpty || value.trim.equalsIgnoreCase("null")

  val weekValidator: String => (String => Int) => Int = value =>
    weekFunction => {
      if (nullChecker(value)) {
        0
      } else {
        weekFunction(value)
      }
    }

  val dateValidator: String => (String => String) => String = value =>
    dateFunction => {
      if (nullChecker(value)) {
        value
      } else {
        dateFunction(value)
      }
    }


}
