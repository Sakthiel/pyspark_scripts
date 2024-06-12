from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from config.definitions import DATA_DIR
spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(f"{DATA_DIR}/Marvel+Names")

lines = spark.read.text(f"{DATA_DIR}/Marvel+Graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

heroWithOneConnection = connections.filter(connections.connections == 1)

heroWithOneConnection.show()

joined_df = heroWithOneConnection.join(names , "id").select("name" , "connections")

joined_df.show()

minConnectionsCount = connections.agg(func.min("connections"))
minConnectionsCount.show()

most_obscure_hero_id = connections.sort(func.col("connections")).first()[0]
most_obscure_hero = names.filter(names.id == most_obscure_hero_id).first()

print("Most obscure hero is " + str(most_obscure_hero[1]))
spark.stop()