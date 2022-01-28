import pyspark.sql.functions as F
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from pyspark.sql.window import Window
import time



def empty_rdd_check():
    print("Empty RDD")

def process_rdd(rdd):
     messages = rdd.collect()
     for msg in messages:
         txtFile = sc.textFile(f"hdfs://{msg}")
         mapper_rdd = txtFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
         mapper_df = mapper_rdd.toDF()
         mapper_df = mapper_df.withColumnRenamed("_1", "word").withColumnRenamed("_2", "count")
         final_df = mapper_df.groupBy("word").agg(F.sum("count").alias("count"))
         print("Final DF")
         final_df.show()
         print(final_df.count())
         window = Window.orderBy(F.col("count").desc())
         final_df = final_df.withColumn("row_number", F.row_number().over(window))
         final_df.filter(F.col("row_number")<=100).select("word","count").repartition(1).write.parquet(f"/user/root/top_words/{msg.split('/')[-1]}")
         


if __name__ == '__main__':
     spark = SparkSession.builder.getOrCreate()
     sc = spark.sparkContext

     ssc = StreamingContext(sc, 30)
     rdd_direct_stream = KafkaUtils.createDirectStream(ssc, ['top_words'], {'metadata.broker.list':'10.130.8.11:9092'})
     rdd_stream_data = rdd_direct_stream.map(lambda m: m[1])
     rdd_stream_data.foreachRDD(lambda rdd: empty_rdd_check() if rdd.count() == 0 else process_rdd(rdd))

     ssc.start()
     ssc.awaitTermination()

