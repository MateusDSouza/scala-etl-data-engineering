# Bitcoin Price Analysis with Apache Spark

## Overview

This project was developed to practice the **functional programming paradigm** in creating data processing application with **Scala**. The application processes Bitcoin price data in USD, extracted from the **Binance API**. Each record represents a 1-minute time frame.

The pipeline includes data extraction, transformation, and saving the results as a CSV file. Key features include renaming columns to camel case, formatting timestamps, and calculating derived metrics.

## Features

- **Functional Programming Paradigm**: Pure functions and a transformation pipeline.
- **Apache Spark**: Scalable data processing.
- **Data Enrichment**: Adds derived metrics like maximum trade range and positive close flag.
- **Column Normalization**: Converts column names to camel case.
- **Date Formatting**: Converts timestamp columns to a human-readable format.

## Project Structure

- `src/main/scala/ETL.scala`: Main application logic.
- Input file: `data/bitcoin-price.csv`
- Output folder: `output`.

### Prerequisites

1. **Scala**: 2.13.15.
2. **SBT**: 1.10.6.
3. **Apache Spark**: 3.5.3.
