package it.mateusdesouza.spark
package load

import org.apache.spark.sql.DataFrame

/** Object for handling CSV file loading operations.
 *
 * This object extends the `CSVLoader` trait, inheriting its functionality to display
 * and save DataFrames as CSV files to a specified output path.
 *
 * @see [[CSVLoader]]
 */
object Loader extends CSVLoader {

  /** Path to the output CSV file containing transformed data. */
  val outputPath: String = "output/output-data.csv"

  final def apply(transformedDf: DataFrame): Unit = saveData(df = transformedDf , outputPath = this.outputPath)
}
