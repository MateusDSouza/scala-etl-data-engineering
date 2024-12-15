package it.mateusdesouza.spark
package config

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File

/**
 * An object for consolidating Spark and ETL configuration loading.
 *
 * This object combines the functionalities of both [[SparkConfig]] and [[ETLConfig]] traits.
 * It provides a unified interface to access configuration parameters related to Spark setup
 * and ETL operations. The configuration values are retrieved from specific configuration
 * files defined in the respective traits.
 *
 * @see [[SparkConfig]] for Spark-related configurations.
 * @see [[ETLConfig]] for ETL-related configurations.
 */

object ConfigLoader extends SparkConfig with ETLConfig
