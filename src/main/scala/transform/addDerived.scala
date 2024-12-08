package it.mateusdesouza.spark
package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * A trait that provides functionality to add derived columns to a DataFrame for analysis.
 *
 * This trait defines methods to calculate additional columns that enhance the dataset
 * with computed metrics or boolean indicators useful for analysis.
 */
trait addDerived {
  /** Adds derived columns for analysis.
   *
   * @param df Dataframe.
   * @return Dataframe with derived columns.
   */
  final def addDerivedColumns(df: DataFrame): DataFrame = {
    df.withColumn("maxTrade", col("high") - col("low"))
      .withColumn("closePositive", col("open") < col("close"))
  }

}
