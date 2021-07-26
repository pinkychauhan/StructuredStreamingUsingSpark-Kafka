#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import from_json
from pyspark.sql.functions import explode, split, concat_ws

spark = (SparkSession.builder
         .appName("ccc_task2_g1q1")
         .enableHiveSupport()
         .getOrCreate())

ds = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "b-1.cs598task2kafkacluster.cw1k16.c1.kafka.us-east-1.amazonaws.com:9092,b-2.cs598task2kafkacluster.cw1k16.c1.kafka.us-east-1.amazonaws.com:9092")
      .option("subscribe", "cs598task2topic")
      .option("startingOffsets", "earliest")
      .load().selectExpr("CAST(value AS STRING)"))

ds.printSchema()

schema = StructType([     StructField("Year",StringType(),True),
                          StructField("Month",StringType(),True),
                          StructField("DayofMonth", StringType(), True),
                          StructField("DayOfWeek", StringType(), True),
                          StructField("FlightDate", DateType(), True),
                          StructField("UniqueCarrier",StringType(),True),
                          StructField("FlightNum",StringType(),True),
                          StructField("Origin",StringType(),True),
                          StructField("Dest", StringType(), True),
                          StructField("CRSDepTime", StringType(), True),
                          StructField("DepDelay",StringType(),True),
                          StructField("ArrDelay", StringType(), True),
                          StructField("Cancelled", StringType(), True)     ])

# Create dataframe setting schema for event data
df_aviation = ds.withColumn("value", from_json("value", schema))

df_aviation = (df_aviation
               .withColumn("Origin", df_aviation["value.Origin"].cast(StringType()))
               .withColumn("Dest", df_aviation["value.Dest"].cast(StringType())))

df_aviation_g1q1 = df_aviation.select(concat_ws(' ', df_aviation["Origin"], df_aviation["Dest"]).alias("OrigOrDest"))
df_aviation_g1q1.printSchema()
airports = df_aviation_g1q1.select(
    explode(
        split(df_aviation_g1q1["OrigOrDest"], " ")
    ).alias("airport")
)
# Generate running airport count
airportsPopularityCounts = airports.groupBy("airport").count()

def for_each_batch(df, epoch_id):
    df.orderBy(df["count"].desc()).limit(10).write.mode("overwrite").saveAsTable("g1_q1")
    spark.sql("REFRESH TABLE g1_q1")
    spark.sql("select * from g1_q1").show()


query = (airportsPopularityCounts
         .writeStream
         .outputMode("complete")
         .foreachBatch(for_each_batch)
         .start())

query.awaitTermination()

spark.sql("REFRESH TABLE g1_q1")
spark.sql("select * from g1_q1").show()



