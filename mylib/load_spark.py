
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import when, col
import pandas as pd
def extract_spark():
    url="https://raw.githubusercontent.com/fivethirtyeight/data/refs/heads/master/college-majors/grad-students.csv"
    file_path="data/grad-students.csv"
    spark = SparkSession.builder.appName("grade_student").getOrCreate() # start spark
    # spark_data = spark.read.csv(file_path) # load as csv
    df=pd.read_csv(url)
    spark_df = spark.createDataFrame(df) 
    spark_df.write.format("delta").mode("append").saveAsTable("grade_student_delta")
    
    return file_path
     

def sql_load(table="grade_student_delta"):
    spark = SparkSession.builder.appName("grade_student").getOrCreate() 
    query_result=spark.sql("""select *
            from grade_student_delta
            group by Major_category"""
    )

    return



if __name__ == "__main__":
    extract_spark()
    #load_spark()
   
    

    
    
    
    

    