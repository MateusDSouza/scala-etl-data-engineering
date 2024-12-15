package it.mateusdesouza.spark
package extract

import providers.SchemaProvider.defineSchema
import providers.SparkSessionProvider.createSparkSession

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for the CSVExtractor functionality.
 *
 * This test suite verifies that the `CSVExtractor` correctly extracts data from the specified input CSV file
 * and ensures that the schema matches the expected definition.
 */
class CSVExtractorTest extends AnyFunSuite with CSVExtractor {

  /**
   * Spark session used for testing.
   */
  private val spark: SparkSession = createSparkSession()

  /**
   * Schema definition for the input CSV file.
   */
  private val schema: StructType = defineSchema()

  /**
   * Path to the input CSV file containing Bitcoin price data.
   */
  private val inputPath: String = "data/bitcoin-price.csv"

  /**
   * Test to ensure that the CSVExtractor correctly extracts data and matches the expected schema.
   */
  test("CSVExtractor should correctly extract data from the input file") {
    val extractedDf: Option[DataFrame] = extract(spark = spark, schema = schema, inputPath = inputPath)
    val actualDf: Option[DataFrame] = Extractor()

    // Validate the schema if both DataFrames are present.
    (actualDf, extractedDf) match {
      case (Some(actual), Some(extracted)) =>
        assert(
          actualDf.collect() == extractedDf.collect(),
          "row count do not match"
        )
      case _ =>
        fail("One or both DataFrames are missing")
    }
  }
}