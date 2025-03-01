# BIG-DATA-ANALYSIS

# Big Data Analysis Using PySpark

## Instructions
Perform analysis on a large dataset using tools like PySpark or Dask to demonstrate scalability.

## Deliverable
A script or notebook with insights derived from big data processing.

## Project Overview
This project demonstrates how to perform big data analysis using PySpark on the New York City Taxi Trip Record dataset. The analysis includes data cleaning, exploratory data analysis (EDA), and visualization of key insights.

## Prerequisites
Before running the script, ensure you have the following installed:
1. Python 3.x
2. PySpark
3. Pandas
4. Matplotlib
5. PyArrow

Install the required packages using:
```bash
pip install pyspark pandas matplotlib pyarrow
```

## Hadoop Winutils Setup
For Windows users, set up Hadoop winutils:
1. Download winutils.exe from: https://github.com/steveloughran/winutils
2. Create directory: `C:\hadoop\bin`
3. Place winutils.exe in `C:\hadoop\bin`
4. Set environment variables:
   ```bash
   setx HADOOP_HOME "C:\hadoop"
   setx PATH "%PATH%;C:\hadoop\bin"
   ```

## Project Structure
- `pyspark_analysis.py`: Main analysis script
- `README.md`: Project documentation

## Analysis Steps
The script performs the following steps:

### 1. Environment Setup
- Initializes SparkSession with optimized configurations:
  ```python
  SparkSession.builder \
      .appName("Big Data Analysis with PySpark") \
      .config("spark.driver.memory", "8g") \
      .config("spark.executor.heartbeatInterval", "120s") \
      .config("spark.network.timeout", "240s") \
      .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
      .config("spark.sql.debug.maxToStringFields", "100") \
      .getOrCreate()
  ```

### 2. Data Loading
- Loads the NYC Taxi Trip dataset from a CSV file:
  ```python
  df = spark.read.csv(data_path, header=True, inferSchema=True)
  ```

### 3. Data Preprocessing
- Cleans the data by:
  - Removing rows with null values in critical columns
  - Filtering out trips with zero or negative values
  - Calculating trip duration

### 4. Exploratory Data Analysis (EDA)
- Computes basic statistics:
  ```python
  df_clean.select(numeric_columns).describe().show()
  ```

### 5. Data Analysis
- Calculates average trip distance and duration
- Identifies top pickup and drop-off locations
- Analyzes trip distribution by hour

### 6. Visualization
- Generates visualizations using Matplotlib:
  - Trip distribution by hour of day
  - Average fare amount by passenger count

## Running the Script
Execute the analysis script:
```bash
python pyspark_analysis.py
```

## Expected Output
The script will output:
1. Data cleaning statistics
2. Basic trip metrics
3. Top pickup and drop-off locations
4. Hourly trip distribution
5. Visualizations in separate windows

## Configuration Details
- **Spark Configurations**:
  - `spark.driver.memory`: 8GB allocated for driver memory
  - `spark.executor.heartbeatInterval`: 120 seconds
  - `spark.network.timeout`: 240 seconds
  - `spark.sql.execution.arrow.pyspark.enabled`: True for optimized Pandas conversions
  - `spark.sql.debug.maxToStringFields`: 100 to prevent plan truncation

## Troubleshooting
- **Hadoop Warnings**: Ensure winutils.exe is properly set up
- **Visualization Errors**: Verify Matplotlib and Pandas installations
- **Performance Issues**: Adjust memory configurations based on your system resources

## References
- PySpark Documentation: https://spark.apache.org/docs/latest/api/python/
- NYC Taxi Data: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Matplotlib Documentation: https://matplotlib.org/stable/contents.html
