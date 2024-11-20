import kagglehub
import pyspark
from pyspark.sql import SparkSession
import os
import pandas as pd
# Download latest version
spark = SparkSession.builder \
    .appName("FinancialDataAnalysis") \
    .getOrCreate()
path = kagglehub.dataset_download("franciscogcc/financial-data")

print("Path to dataset files:", path)
dataset_files = os.listdir(path)
print("Files in dataset:", dataset_files)

dataset_file_path = os.path.join(path, "financial_regression.csv")

spark_df=spark.read.csv(dataset_file_path,header=True, inferSchema=True)
spark_df.printSchema()
spark_df.show(5)

output_csv_path='gold_dataset.csv'
spark_df.write.csv(output_csv_path,header=True)

