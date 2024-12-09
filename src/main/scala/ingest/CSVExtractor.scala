package it.mateusdesouza.spark
package ingest

import config.ConfigLoader

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Trait for extracting data from a CSV file.
 *
 * This trait provides a method to read and extract data from a CSV file using Spark,
 * with a specified schema to structure the input data. */
trait CSVExtractor {
  /** Reads the data from the input path.
   *
   * @param spark     Spark Session.
   * @param inputPath Path to data.
   * @param schema    Schema of input data.
   * @return Dataframe extracted. */
  def extract(spark: SparkSession, inputPath: String, schema: StructType): Option[DataFrame] = try {
    Some(spark.read.option("header", value = true).schema(schema).csv(ConfigLoader.getInputPath))
  } catch {
    case e: Exception => println {
      s"Error reading data: ${e.getMessage}"
    }
      None
  }
}
