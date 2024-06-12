from pyspark.sql import SparkSession
from pyspark.sql.types import StructType , StructField , StringType , FloatType , IntegerType
from pyspark.sql import functions as f
from config.definitions import DATA_DIR

spark  = SparkSession.builder.appName("totalAmount").getOrCreate()

customer_schema = StructType([ \
            StructField("c_id", IntegerType() , True) , \
            StructField("item_id", IntegerType() , True ,) ,
            StructField("amount", FloatType() , True)])

customer = spark.read.schema(customer_schema).csv(f"{DATA_DIR}/customer-orders.csv")

customerWithAmount = customer.select("c_id" , "amount")

customerWithAmount = customerWithAmount.groupby("c_id").agg(f.round(f.sum("amount") , 2) \
                                                            .alias("amount_spent")) \
                                                            .sort("amount_spent")
customerWithAmount.show()

spark.stop()