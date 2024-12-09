package it.mateusdesouza.spark
package providers

import config.ConfigLoader

import org.apache.spark.sql.SparkSession

/**
 * Provides a utility method for creating and managing a SparkSession instance.
 * It defines a concrete implementation
 * of a method to initialize a local SparkSession with pre-configured settings.
 *
 *
 */
object SparkSessionProvider {
  /** Creates a Spark session. */
  final def createSparkSession(): SparkSession = SparkSession.builder()
    .appName(ConfigLoader.getAppName)
    .master(ConfigLoader.getMaster)
    .config(ConfigLoader.getSparkDriverAddressKey, ConfigLoader.getSparkDriverAddressValue)
    .getOrCreate()
}
