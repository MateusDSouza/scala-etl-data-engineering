package it.mateusdesouza.spark
package providers

import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

/**
 * Provides the schema definition for the input data used in the pipeline.
 *
 * The schema represents data related to Bitcoin price analysis, with fields such as
 * open time, close time, price metrics (open, high, low, close), volume, and trade details.
 */
object SchemaProvider {

  /**
   * Defines the schema for the input data.
   *
   * The schema includes the following fields:
   *  - `Open Time` (String): The timestamp for the opening of the trading period.
   *  - `Open` (Double): The opening price for the trading period.
   *  - `High` (Double): The highest price during the trading period.
   *  - `Low` (Double): The lowest price during the trading period.
   *  - `Close` (Double): The closing price for the trading period.
   *  - `Volume` (Float): The trading volume during the trading period.
   *  - `Close Time` (String): The timestamp for the close of the trading period.
   *  - `Quote asset volume` (Double): The total volume of the quote asset traded.
   *  - `Number of trades` (Integer): The total number of trades during the period.
   *  - `Taker buy base asset volume` (Float): The volume of the base asset bought by takers.
   *  - `Taker buy quote asset volume` (Double): The volume of the quote asset bought by takers.
   *
   * @return The schema definition as a [[org.apache.spark.sql.types.StructType]].
   */
  final def defineSchema(): StructType = {
    StructType(Array(
      StructField("Open Time", StringType, nullable = true),
      StructField("Open", DoubleType, nullable = true),
      StructField("High", DoubleType, nullable = true),
      StructField("Low", DoubleType, nullable = true),
      StructField("Close", DoubleType, nullable = true),
      StructField("Volume", FloatType, nullable = true),
      StructField("Close Time", StringType, nullable = true),
      StructField("Quote asset volume", DoubleType, nullable = true),
      StructField("Number of trades", IntegerType, nullable = true),
      StructField("Taker buy base asset volume", FloatType, nullable = true),
      StructField("Taker buy quote asset volume", DoubleType, nullable = true)
    ))
  }
}
