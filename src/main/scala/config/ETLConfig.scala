package it.mateusdesouza.spark
package config

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

/**
 * A trait for loading and accessing ETL-related configuration parameters.
 *
 * This trait provides methods to retrieve essential ETL configuration values such as
 * the input and output paths for the data. The configuration is read from a file
 * specified by the `configPath` variable.
 *
 * The configuration file must be in HOCON format, and its default path is
 * `src/main/resources/etl_params.conf`.
 *
 * @note The `configPath` variable can be updated to point to a different configuration file location if necessary.
 */
trait ETLConfig {

  /** The path to the configuration file. */
  private val configPath: String = "src/main/resources/etl_params.conf"

  /** The loaded configuration object. */
  private val config: Config = ConfigFactory.parseFile(new File(configPath))

  /**
   * Retrieves the input path of the data specified in the configuration file.
   *
   * @return A string representing the data input path.
   */
  final def getInputPath: String = config.getString("inputPath")

  /**
   * Retrieves the output path of the data specified in the configuration file.
   *
   * @return A string representing the data output path.
   */
    //TODO: FIX THIS BUG
  final def getOutputPath: String = config.getString("OutputPath")
}