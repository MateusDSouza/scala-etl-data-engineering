package it.mateusdesouza.spark
package config
import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

/**
 * A trait for loading and accessing Spark configuration parameters.
 *
 * This trait provides methods to retrieve essential Spark-related configurations
 * such as application name, master URL, and Spark driver address. It reads
 * configuration values from a file specified by the `configPath` variable.
 *
 * The configuration file must be in HOCON format, and its default path is
 * `src/main/resources/spark_params.conf`.
 *
 * @note The `configPath` variable can be updated if a different configuration file location is needed.
 */
trait SparkConfig {
  /** The path to the configuration file. */
  private val configPath: String = "src/main/resources/spark_params.conf"

  /** The loaded configuration object. */
  private val config: Config = ConfigFactory.parseFile(new File(configPath))

  /**
   * Retrieves the application name specified in the configuration file.
   *
   * @return A string representing the application name.
   */
  final def getAppName: String = config.getString("app-name")

  /**
   * Retrieves the master URL specified in the configuration file.
   *
   * @return A string representing the master URL.
   */
  final def getMaster: String = config.getString("master")

  /**
   * Retrieves the key for the Spark driver bind address specified in the configuration file.
   *
   * @return A string representing the Spark driver address key.
   */
  final def getSparkDriverAddressKey: String = config.getString("spark-driver-address.key")

  /**
   * Retrieves the value for the Spark driver bind address specified in the configuration file.
   *
   * @return A string representing the Spark driver address value.
   */
  final def getSparkDriverAddressValue: String = config.getString("spark-driver-address.value")

}
