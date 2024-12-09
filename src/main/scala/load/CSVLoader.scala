package it.mateusdesouza.spark
package load

import config.ConfigLoader

import org.apache.spark.sql.DataFrame

/** Trait for loading and saving data to a CSV file.
 *
 * This trait provides functionality to display the contents of a transformed DataFrame
 * and save it to a specified output path in CSV format.
 */
trait CSVLoader {
  /** Shows and saves the transformed DataFrame to the output path.
   *
   * @param df         Dataframe transformed.
   * @param outputPath Save destination.
   */
  def saveData(df: DataFrame, outputPath: String): Unit = {
    df.show()
    try {
      df.write.option("sep", ",")
        .option("header", "true")
        .option("escape", "\"")
        .option("quoteAll", "true").csv(ConfigLoader.getOutputPath)
    } catch {
      case e: Exception =>
        println(s"Error saving data: ${e.getMessage}")
    }
  }
}
