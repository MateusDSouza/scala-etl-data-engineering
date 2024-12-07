package it.mateusdesouza.spark

import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * Object responsible for processing Bitcoin price data from a CSV file using Spark.
 * The pipeline includes reading data, applying transformations, and saving the results.
 */
object Main {

  /** Main entry point for the application. */
  def main(args: Array[String]): Unit = {

    /**
     * Initializes the Spark session for the application.
     */
    val spark: SparkSession = createSparkSession()

    /**
     * Path to the input CSV file containing Bitcoin price data.
     */
    val inputPath = "data/bitcoin-price.csv"

    /**
     * Path to the output directory where the processed data will be saved.
     */
    val outputPath = "output/output-data.csv"

    /**
     * Schema definition for the input CSV file.
     */
    val inputSchema = defineSchema()

    /**
     * Pipeline for data processing, implemented using a `for`-comprehension.
     * Each step applies a transformation to the DataFrame:
     *  - `readData`: Reads the input data.
     *  - `renameColumnsToCamelCase`: Converts column names to camelCase format.
     *  - `formatDateColumns`: Formats specific columns to readable date formats.
     *  - `addDerivedColumns`: Adds calculated columns to the DataFrame.
     *
     * If any step fails (returns `None`) and the pipeline terminates.
     *
     * After, matches the result of the pipeline to determine success or failure:
     *  - If the pipeline succeeds, the resulting DataFrame is displayed and saved to the output path.
     *  - If the pipeline fails (returns `None`), a failure message is printed to the console.
     */
    val pipeline = for {
      rawDf <- readData(spark, inputPath, inputSchema)
      camelCaseDf <- Some(renameColumnsToCamelCase(rawDf))
      dateFormattedDf <- Some(formatDateColumns(camelCaseDf, Seq("openTime", "closeTime")))
      finalDf <- Some(addDerivedColumns(dateFormattedDf))
    } yield finalDf

    pipeline match {
      case Some(df) =>
        df.show()
        saveData(df, outputPath)
      case None => println("Pipeline failed")
    }
  }

  /** Creates a Spark session. */
  private def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Bitcoin Price Analysis")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  /** Defines the schema for the input data. */
  private def defineSchema(): StructType = {
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

  /** Reads the data from the input path.
   *
   * @param spark     Spark Session.
   * @param inputPath Path to data.
   * @param schema    Schema of input data.
   * @return Dataframe extracted.
   */
  private def readData(spark: SparkSession, inputPath: String, schema: StructType): Option[DataFrame] = {
    try {
      Some(spark.read.option("header", value = true).schema(schema).csv(inputPath))
    } catch {
      case e: Exception =>
        println(s"Error reading data: ${e.getMessage}")
        None
    }
  }

  /** Renames all columns in the DataFrame to camel case.
   *
   * @param df Dataframe.
   * @return Dataframe with columns in camel case.
   */
  private def renameColumnsToCamelCase(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((tempDF, colName) => tempDF.withColumnRenamed(colName, toCamelCase(colName)))
  }

  /** Converts column names to camel case.
   *
   * @param column column name.
   * @return Column name in camel case.
   */
  private def toCamelCase(column: String): String = {
    column.split("\\s+|_|-").zipWithIndex.map { case (word, index) =>
      if (index == 0) word.toLowerCase else word.capitalize
    }.mkString("")
  }

  /** Formats specified columns as dates.
   *
   * @param df      Dataframe.
   * @param columns Sequence of column names.
   * @return Dataframe with date columns formatted.
   */
  private def formatDateColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df) { (tempDf, colName) =>
      tempDf.withColumn(colName, toDate(colName))
    }
  }

  /** Converts a timestamp column from long to a readable date.
   *
   * @param colName Column name to be transformed.
   * @return Column with formatted date.
   */
  private def toDate(colName: String): Column = from_unixtime(col(colName) / 1000)

  /** Adds derived columns for analysis.
   *
   * @param df Dataframe.
   * @return Dataframe with derived columns.
   */
  private def addDerivedColumns(df: DataFrame): DataFrame = {
    df.withColumn("maxTrade", col("high") - col("low"))
      .withColumn("closePositive", col("open") < col("close"))
  }

  /** Shows and saves the transformed DataFrame to the output path.
   *
   * @param df         Dataframe transformed.
   * @param outputPath Save destination.
   */
  private def saveData(df: DataFrame, outputPath: String): Unit = {
    df.show()
    try {
      df.repartition(1)
        .write.option("sep", ",")
        .option("header", "true")
        .option("escape", "\"")
        .option("quoteAll", "true").csv(outputPath)
    } catch {
      case e: Exception =>
        println(s"Error saving data: ${e.getMessage}")
    }
  }
}
