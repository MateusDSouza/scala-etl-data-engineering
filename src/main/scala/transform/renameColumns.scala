package it.mateusdesouza.spark
package transform

import org.apache.spark.sql.DataFrame

/**
 * A trait that provides functionality for renaming DataFrame columns to camel case.
 *
 * This trait can be mixed into classes or objects to enable column renaming, ensuring consistency in column naming conventions.
 */
trait renameColumns {

  /** Converts column names to camel case.
   *
   * @param column column name.
   * @return Column name in camel case.
   */
  private def toCamelCase(column: String): String = {
    column.split("\\s+|_|-").zipWithIndex.map { case (word, index) =>
      if (index == 0) word.toLowerCase else word.capitalize
    }.mkString("")
  }

  /** Renames all columns in the DataFrame to camel case.
   *
   * @param df Dataframe.
   * @return Dataframe with columns in camel case.
   */
  final def renameColumnsToCamelCase(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((tempDF, colName) => tempDF.withColumnRenamed(colName, toCamelCase(colName)))
  }
}
