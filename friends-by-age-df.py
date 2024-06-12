from pyspark.sql import SparkSession
from config.definitions import DATA_DIR
from pyspark.sql import functions as f
spark = SparkSession.builder.appName("friends-by-age").getOrCreate()

people = spark.read.option("header" ,"true").option("inferSchema" , "true")\
        .csv(f"{DATA_DIR}/fakefriends-header.csv")
people.printSchema()

ageWithFriends = people.select( people.age , people.friends)
ageWithFriends.groupby(ageWithFriends.age).agg(f.avg("friends").alias("friends")).sort("age").show()

spark.stop()