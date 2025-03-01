"""
Big Data Analysis Using PySpark

Objective: Perform analysis on a large dataset using PySpark to demonstrate scalability.
"""

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour, to_timestamp, desc, unix_timestamp
import matplotlib.pyplot as plt
import os

def initialize_spark_session():
    """
    Initialize and return a SparkSession with appropriate configurations.
    """
    # Set Hadoop home directory and winutils path
    os.environ['HADOOP_HOME'] = 'C:/hadoop'
    os.environ['PATH'] = f"{os.environ['HADOOP_HOME']}/bin;{os.environ['PATH']}"
    
    return SparkSession.builder \
        .appName("Big Data Analysis with PySpark") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.heartbeatInterval", "120s") \
        .config("spark.network.timeout", "240s") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

def load_data(spark, data_path):
    """
    Load dataset into a DataFrame and return it.
    """
    return spark.read.csv(data_path, header=True, inferSchema=True)

def preprocess_data(df):
    """
    Clean and preprocess the data.
    """
    # Display the number of rows before cleaning
    print(f"Number of rows before cleaning: {df.count()}")

    # Drop rows with null values in critical columns
    cols_to_check = ["passenger_count", "trip_distance", "fare_amount"]
    df_clean = df.dropna(subset=cols_to_check)

    # Filter out trips with zero or negative values
    df_clean = df_clean.filter(
        (col("passenger_count") > 0) &
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0)
    )

    # Display the number of rows after cleaning
    print(f"Number of rows after cleaning: {df_clean.count()}")
    return df_clean

def perform_eda(df_clean):
    """
    Perform exploratory data analysis.
    """
    # Basic statistics
    numeric_columns = ["passenger_count", "trip_distance", "fare_amount", "tip_amount"]
    df_clean.select(numeric_columns).describe().show()

    # Trip duration calculation
    df_clean = df_clean.withColumn(
        "trip_duration",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
    )
    df_clean = df_clean.filter(
        (col("trip_duration") > 0) & (col("trip_duration") <= 120)
    )
    
    # Extract pickup hour
    df_clean = df_clean.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
    
    return df_clean

def analyze_data(df_clean):
    """
    Perform data analysis and generate insights.
    """
    # Average trip distance and duration
    avg_distance = df_clean.agg(avg("trip_distance")).first()[0]
    avg_duration = df_clean.agg(avg("trip_duration")).first()[0]
    print(f"\nAverage Trip Distance: {avg_distance:.2f} miles")
    print(f"Average Trip Duration: {avg_duration:.2f} minutes")

    # Busiest pickup and drop-off locations
    print("\nTop 5 Pickup Locations:")
    top_pickup = df_clean.groupBy("PULocationID").count().orderBy(desc("count")).limit(5)
    top_pickup.show()

    print("\nTop 5 Drop-off Locations:")
    top_dropoff = df_clean.groupBy("DOLocationID"). count().orderBy(desc("count")).limit(5)
    top_dropoff.show()

    # Peak hours analysis
    print("\nTrip Count by Hour:")
    trips_by_hour = df_clean.groupBy("pickup_hour").count().orderBy("pickup_hour")
    trips_by_hour.show(24)

def visualize_results(df_clean):
    """
    Visualize the analysis results.
    """
    try:
        # Trip counts by hour
        trips_by_hour_pd = df_clean.groupBy("pickup_hour").count().orderBy("pickup_hour").toPandas()
        plt.figure(figsize=(10,6))
        plt.bar(trips_by_hour_pd['pickup_hour'], trips_by_hour_pd['count'], color='skyblue')
        plt.xlabel('Hour of Day')
        plt.ylabel('Number of Trips')
        plt.title('NYC Taxi Trips by Hour')
        plt.xticks(range(0,24))
        plt.show()

        # Average fare amount by passenger count
        avg_fare_by_passenger = df_clean.groupBy("passenger_count").agg(avg("fare_amount").alias("avg_fare")).orderBy("passenger_count")
        avg_fare_by_passenger_pd = avg_fare_by_passenger.toPandas()
        plt.figure(figsize=(8,5))
        plt.bar(avg_fare_by_passenger_pd['passenger_count'], avg_fare_by_passenger_pd['avg_fare'], color='salmon')
        plt.xlabel('Passenger Count')
        plt.ylabel('Average Fare Amount ($)')
        plt.title('Average Fare by Passenger Count')
        plt.show()
    except Exception as e:
        print(f"Visualization error: {str(e)}")
        print("Please ensure you have the required visualization dependencies installed:")
        print("pip install matplotlib pandas")

def main():
    # Initialize Spark session
    spark = initialize_spark_session()

    try:
        # Load data
        data_path = "C:/Users/PIYUSH/Desktop/Outdoor Adventurous Tourism/yellow_tripdata_2022-01.csv"
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Data file not found at: {data_path}")
            
        df = load_data(spark, data_path)

        # Preprocess data
        df_clean = preprocess_data(df)

        # Perform EDA
        df_clean = perform_eda(df_clean)

        # Analyze data
        analyze_data(df_clean)

        # Visualize results
        visualize_results(df_clean)

    except Exception as e:
        print(f"Error during execution: {str(e)}")
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()
