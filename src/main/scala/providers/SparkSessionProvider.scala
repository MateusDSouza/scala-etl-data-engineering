package it.mateusdesouza.spark
package providers

import org.apache.spark.sql.SparkSession
/**
 * Provides a utility method for creating and managing a SparkSession instance.
 * It defines a concrete implementation
 * of a method to initialize a local SparkSession with pre-configured settings.
 *

 */
object SparkSessionProvider {
  /** Creates a Spark session. */
  final def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("Bitcoin Price Analysis")
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
}
