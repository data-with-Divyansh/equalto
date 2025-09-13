import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\Quantumn\.jdks\corretto-1.8.0_462'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()

print("====================== pyspark started ==================")
print()

df = spark.read.csv("equalto.csv",sep="=").withColumnRenamed("_c0","columns").withColumnRenamed("_c1","details")
df.show()

df1 = (df.withColumn("details",split(col("details"),"\\|"))
       .selectExpr("columns","posexplode(details)as(pos,value)")
       )
df1.show()

fdf = (df1.groupBy("pos").pivot("columns").agg(first("value")).drop("pos")
       .selectExpr("Worker_ID","Name","Department","Salary")
       .na.replace("",None)
       )
fdf.show()













