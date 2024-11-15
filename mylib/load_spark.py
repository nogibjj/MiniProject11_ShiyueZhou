
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import when, col
def extract_spark():
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/grad-students.csv"
    file_path="data/grad-students.csv"
    spark = SparkSession.builder.appName("grade_student").getOrCreate() # start spark
    spark_data = spark.read.csv(file_path, inferSchema=True, header=True) # load as csv
    spark_df = spark_data.createDataFrame() 
    spark_df.write.format("delta").mode("append").saveAsTable("grade_student_delta")
    
    return file_path
    
# def load_spark(dataset=""):
#     spark=
#     spark_df.write.format("delta").mode("overwrite").saveAsTable("grade_student_delta")
       
# def start_spark(appName="grade_student"):
#     spark = SparkSession.builder.appName(appName).getOrCreate()
#     return spark

# def end_spark(spark):
#     try:
#         spark.stop()
#         return "Spark session stopped successfully."
#     except Exception as e:
#         return f"Error stopping Spark session: {e}"

# def load(spark, file_path="data/grad-students.csv"):
#     # Start Spark session
#     try:
#         # Load the CSV data
#         spark_data = spark.read.csv(file_path, inferSchema=True, header=True)
        
#         return spark_data
#     except Exception as e:
#         print(f"Error reading CSV: {e}")
#         return None
    

if __name__ == "__main__":
    extract_spark()
    #load_spark()
   
    

    
    
    
    

    