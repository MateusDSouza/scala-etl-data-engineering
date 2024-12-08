package it.mateusdesouza.spark
package ingest
import providers.SchemaProvider.defineSchema
import providers.SparkSessionProvider.createSparkSession

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Utility object for handling CSV data ingestion.
 *
 * This object provides functionality to read and load CSV data into a Spark DataFrame
 * with the specified schema. It is designed to handle potential exceptions gracefully
 * and return an `Option` to indicate success or failure.
 *
@see [[CSVExtractor]]
 */
object Extractor extends CSVExtractor {

  /**
   * Initializes the Spark session for the application.
   */
  val spark: SparkSession = createSparkSession()

  /**
   * Schema definition for the input CSV file.
   */
  val schema: StructType = defineSchema()

  /**
   * Path to the input CSV file containing Bitcoin price data.
   */
  val inputPath: String = "data/bitcoin-price.csv"

  final def apply(): Option[DataFrame] = extract(spark = this.spark, inputPath = this.inputPath, schema = this.schema)

}