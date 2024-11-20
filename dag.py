import kagglehub
import os
from pyspark.sql import SparkSession
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Create a Spark session and return it
def create_spark_session():
    spark = SparkSession.builder.appName('Gold_Analysis').getOrCreate()
    return spark

# Download dataset from Kaggle
def download_dataset():
    path = kagglehub.dataset_download("franciscogcc/financial-data")
    print("Path to dataset files:", path)
    return path

# Load CSV data into a Spark dataframe
def load_data_to_spark(path):
    dataset_file_path = os.path.join(path, "financial_regression.csv")
    spark = create_spark_session()  # Use the Spark session
    spark_df = spark.read.csv(dataset_file_path, header=True, inferSchema=True)
    return spark_df

# Clean and filter the data
def clean_data(spark_df):
    cleaned_df = spark_df.dropna()
    filtered_df = cleaned_df.filter(cleaned_df['sp500 close'] > 100)
    return filtered_df

# Save the cleaned data as CSV
def save_data(spark_df):
    output_path = '/home/afb2/gold_regression_project/gold_dataset.csv'  # Corrected file extension
    spark_df.write.csv(output_path, header=True, mode='overwrite')

# Main function to process the dataset
def process_and_save_dataset(**kwargs):
    # Download dataset
    dataset_path = download_dataset()

    # Load data into Spark dataframe
    spark_df = load_data_to_spark(dataset_path)

    # Clean and filter data
    cleaned_data = clean_data(spark_df)

    # Save the cleaned data
    save_data(cleaned_data)

    # Print execution date for logging purposes (optional)
    print(kwargs['execution_date'])  # If you want to use execution_date, otherwise remove

    return 

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 19),
    'retries': 1,
}

# Define the DAG
with DAG(
    'gold_data_processing',
    default_args=default_args,
    description='DAG used to process gold data',
    schedule_interval='@once',  # Adjust as needed (for testing, use @once, for production use interval like '*/5 * * * *')
    catchup=False
) as dag:
    
    # Create the PythonOperator task
    process_and_save = PythonOperator(
        task_id='process_and_save_gold_dataset',
        python_callable=process_and_save_dataset,
        provide_context=True
    )

    process_and_save  # Ensure the task is added to the DAG


