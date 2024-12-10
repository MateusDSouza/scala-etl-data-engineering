package it.mateusdesouza.spark
package config

import org.scalatest.funspec.AnyFunSpec

/**
 * Unit tests for the `ConfigLoader` object.
 *
 * This test suite verifies that the `ConfigLoader` correctly retrieves values
 * from the configuration file for various configuration parameters.
 */
class ConfigLoaderTest extends AnyFunSpec {

  /**
   * To add a new configuration parameter test, include a new entry in the expectedValues map:
   *
   * "New Config Parameter" -> (() => ConfigLoader.getNewConfig, "expectedValue")
   */
  private val expectedValues = Map(
    "Input Path" -> (() => ConfigLoader.getInputPath, "data/bitcoin-price.csv"),
    "Output Path" -> (() => ConfigLoader.getOutputPath, "output/output-data.csv"),
    "App Name" -> (() => ConfigLoader.getAppName, "Bitcoin Price Analysis"),
    "Master" -> (() => ConfigLoader.getMaster, "local[*]"),
    "Spark Driver Address Key" -> (() => ConfigLoader.getSparkDriverAddressKey, "spark.driver.bindAddress"),
    "Spark Driver Address Value" -> (() => ConfigLoader.getSparkDriverAddressValue, "127.0.0.1")
  )

  /**
   * A higher-order function to simplify the testing of configuration values.
   *
   * @param description   A description of the configuration parameter being tested.
   * @param actualValueFn A function to retrieve the actual value.
   * @param expectedValue The expected value to compare against.
   */
  private def testConfigValue(description: String, actualValueFn: () => String, expectedValue: String): Unit = {
    it(s"should return the correct $description from the configuration") {
      val actualValue = actualValueFn()
      assert(actualValue == expectedValue,
        s"Expected $description '$expectedValue', but got '$actualValue'")
    }
  }

  describe("Test ConfigLoader") {
    expectedValues.foreach { case (description, (actualValueFn, expectedValue)) =>
      testConfigValue(description, actualValueFn, expectedValue)
    }
  }
}
