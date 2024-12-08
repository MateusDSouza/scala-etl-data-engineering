package it.mateusdesouza.spark
package transform

import org.apache.spark.sql.DataFrame


object Transformer extends renameColumns with formatDate with addDerived {

/**
 * Applies a series of transformations to a DataFrame.
 *
 * - Renames columns to camel case.
 * - Formats date columns.
 * - Adds derived columns.
 *
 * @param df The input DataFrame.
 * @return The transformed DataFrame.
 */
def apply(df: DataFrame): DataFrame = {
  val camelCaseDF = renameColumnsToCamelCase(df)
  val dateFormattedDF = formatDateColumns(camelCaseDF, Seq("openTime", "closeTime"))
  addDerivedColumns(dateFormattedDF)
}
}