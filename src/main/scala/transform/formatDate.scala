package it.mateusdesouza.spark
package transform

import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.{Column, DataFrame}

/**
 * A trait that provides functionality to format timestamp columns into readable date formats.
 *
 * This trait can be used to apply consistent date formatting transformations to DataFrame columns.
 */
trait formatDate {

  /** Formats specified columns as dates.
   *
   * @param df      Dataframe.
   * @param columns Sequence of column names.
   * @return Dataframe with date columns formatted.
   */
  def formatDateColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df) { (tempDf, colName) =>
      tempDf.withColumn(colName, toDate(colName))
    }
  }

  /** Converts a timestamp column from long to a readable date.
   *
   * @param colName Column name to be transformed.
   * @return Column with formatted date.
   */
  private def toDate(colName: String): Column = from_unixtime(col(colName) / 1000)
}