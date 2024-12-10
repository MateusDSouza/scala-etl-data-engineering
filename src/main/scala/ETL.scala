package it.mateusdesouza.spark

import extract.Extractor
import load.Loader
import transform.Transformer

import org.apache.spark.sql.DataFrame

/**
 * Object responsible for processing Bitcoin price data from a CSV file using Spark.
 * The pipeline includes reading data, applying transformations, and saving the results. */
object ETL {

  /** ETL entry point for the application. */
  def main(args: Array[String]): Unit = {

    /**
     * Pipeline for data processing, implemented using a `for`-comprehension.
     *
     * After, matches the result of the pipeline to determine success or failure:
     *  - If the pipeline succeeds, the resulting DataFrame is displayed and saved to the output path.
     *  - If the pipeline fails (returns `None`), a failure message is printed to the console. */

    val pipeline = {
      Extractor()
        .flatMap(rawDf =>
          Some(Transformer(rawDf))
            .map(finalDf => finalDf)
        )
    }

    pipeline match {
      case Some(df: DataFrame) =>
        Loader {
          df
        }
      case None => println {
        "Pipeline failed"
      }
    }
  }
}