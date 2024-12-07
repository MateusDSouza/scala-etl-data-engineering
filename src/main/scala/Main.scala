package it.mateusdesouza.spark

import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = createSparkSession()
    val inputPath = "data/bitcoin-price.csv"
    val outputPath = "output/output-data.csv"

    val inputSchema = defineSchema()

    val pipeline = for {
      rawDf <- readData(spark, inputPath, inputSchema)
      camelCaseDf <- Some(renameColumnsToCamelCase(rawDf))
      dateFormattedDf <- Some(formatDateColumns(camelCaseDf, Seq("openTime", "closeTime")))
      finalDf <- Some(addDerivedColumns(dateFormattedDf))
    } yield finalDf

    pipeline match {
      case Some(df) => {
        df.show()
        saveData(df, outputPath)
      }
      case None => println("Pipeline failed")
    }
  }

  /** Creates a Spark session */
  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Bitcoin Price Analysis")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }

  /** Defines the schema for the input data */
  def defineSchema(): StructType = {
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

  /** Reads the data from the input path */
  def readData(spark: SparkSession, outputPath: String, schema: StructType): Option[DataFrame] = {
    try {
      Some(spark.read.option("header", value = true).schema(schema).csv(outputPath))
    } catch {
      case e: Exception =>
        println(s"Error reading data: ${e.getMessage}")
        None
    }
  }

  /** Converts column names to camel case */
  def toCamelCase(column: String): String = {
    column.split("\\s+|_|-").zipWithIndex.map { case (word, index) =>
      if (index == 0) word.toLowerCase else word.capitalize
    }.mkString("")
  }

  /** Renames all columns in the DataFrame to camel case */
  def renameColumnsToCamelCase(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)((tempDF, colName) => tempDF.withColumnRenamed(colName, toCamelCase(colName)))
  }

  /** Converts a timestamp column from long to a readable date */
  def toDate(columnName: String): Column = from_unixtime(col(columnName) / 1000)

  /** Formats specified columns as dates */
  def formatDateColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df) { (tempDf, colName) =>
      tempDf.withColumn(colName, toDate(colName))
    }
  }

  /** Adds derived columns for analysis */
  def addDerivedColumns(df: DataFrame): DataFrame = {
    df.withColumn("maxTrade", col("high") - col("low"))
      .withColumn("closePositive", col("open") < col("close"))
  }

  /** Saves the transformed DataFrame to the output path */
  def saveData(df: DataFrame, outputPath: String): Unit = {
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
