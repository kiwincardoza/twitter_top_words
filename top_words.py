import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql.window import Window
import time


def empty_rdd_check(logger):
    logger.info("Empty RDD")

def process_rdd(rdd, logger):
     try:
         messages = rdd.collect()
         for msg in messages:
             logger.info(f"Message Recieved - {msg}")
             txtFile = sc.textFile(f"hdfs://{msg}")    # Load text file into Spark RDD
             mapper_rdd = txtFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))   # Split text into words
             mapper_df = mapper_rdd.toDF()   # Convert RDD to DataFrame
             logger.info("RDD after basic parsing converted into DataFrame")
             mapper_df = mapper_df.withColumnRenamed("_1", "word").withColumnRenamed("_2", "count")
             final_df = mapper_df.groupBy("word").agg(F.sum("count").alias("count"))     # Group and Aggregate by words
             window = Window.orderBy(F.col("count").desc())
             final_df = final_df.withColumn("row_number", F.row_number().over(window))
             # Filter top 100 rows (words) and write as parquet file in HDFS
             final_df.filter(F.col("row_number")<=100).select("word","count").repartition(1).write.parquet(f"/user/root/top_words/{msg.split('/')[-1]}")
             logger.info("top 100 words written into HDFS")
     except Exception as e:
         logger.error(f"Error in processing RDD/Dataframe: {e}")


if __name__ == '__main__':
     spark = SparkSession.builder.getOrCreate()
     sc = spark.sparkContext
     log4j = sc._jvm.org.apache.log4j
     logger = log4j.LogManager.getLogger("top_words")

     try:
         ssc = StreamingContext(sc, 30)   # Batch Stream for every 30 seconds
         # Read stream from Kafka topic
         rdd_direct_stream = KafkaUtils.createDirectStream(ssc, ['top_words'], {'metadata.broker.list':'127.0.0.1:9092'})
         # Read value from Kafka topic partition
         rdd_stream_data = rdd_direct_stream.map(lambda m: m[1])
         rdd_stream_data.foreachRDD(lambda rdd: empty_rdd_check(logger) if rdd.count() == 0 else process_rdd(rdd, logger))

         ssc.start()
         ssc.awaitTermination()
     except Exception as e:
         logger.error(f"Error in main(): {e}")

