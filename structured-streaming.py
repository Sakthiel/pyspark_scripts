# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 09:15:05 2019

@author: Frank
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

from pyspark.sql import functions as func
# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("logs")

# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))


logsDF = logsDF.withColumn("eventTime" , func.current_timestamp())

endpointsCountDF = logsDF.groupBy(func.window(func.col("eventTime") , "30 seconds" , \
                                              "10 seconds") ,\
                                  func.col("endpoint")).count();

endpointsCountDF = endpointsCountDF.orderBy(func.col("count").desc())
# Keep a running count of every access by status code
# statusCountsDF = logsDF.groupBy(logsDF.status).count()

# Kick off our streaming query, dumping results to the console
query = ( endpointsCountDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )

# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()

