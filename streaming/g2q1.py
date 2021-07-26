#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

spark = SparkSession.builder \
    .appName("ccc_task2_g2q1") \
    .enableHiveSupport() \
    .getOrCreate()

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

df_aviation = df_aviation \
    .withColumn("Year", df_aviation["value.Year"].cast(IntegerType())) \
    .withColumn("Month", df_aviation["value.Month"].cast(IntegerType())) \
    .withColumn("DayOfMonth", df_aviation["value.DayOfMonth"].cast(IntegerType())) \
    .withColumn("DayOfWeek", df_aviation["value.DayOfWeek"].cast(IntegerType())) \
    .withColumn("FlightDate", df_aviation["value.FlightDate"].cast(DateType())) \
    .withColumn("UniqueCarrier", df_aviation["value.UniqueCarrier"].cast(StringType())) \
    .withColumn("FlightNum", df_aviation["value.FlightNum"].cast(IntegerType())) \
    .withColumn("Origin", df_aviation["value.Origin"].cast(StringType())) \
    .withColumn("Dest", df_aviation["value.Dest"].cast(StringType())) \
    .withColumn("CRSDepTime", df_aviation["value.CRSDepTime"].cast(IntegerType())) \
    .withColumn("DepDelay", df_aviation["value.DepDelay"].cast(FloatType())) \
    .withColumn("ArrDelay", df_aviation["value.ArrDelay"].cast(FloatType())) \
    .withColumn("Cancelled", df_aviation["value.Cancelled"].cast(FloatType()))

df_aviation.printSchema()

df_aviation_g2q1 = df_aviation.select('Origin','UniqueCarrier', 'DepDelay', 'Cancelled')

df_aviation_g2q1.createOrReplaceTempView("aviation")
carriersByDepDelay = spark.sql("select Origin as SrcAirport, UniqueCarrier as Airline, avg(DepDelay) as AvgDepDelay \
            from aviation where Cancelled = 0 group by SrcAirport, Airline")

def for_each_batch(df, epoch_id):
    window = Window.partitionBy(df['SrcAirport']).orderBy(df['AvgDepDelay'].asc())
    ranked_df = df.select('*', rank().over(window).alias('rank'))
    (ranked_df.filter(col('rank') <= 10).select(df['SrcAirport'], df['Airline'], df['AvgDepDelay'])
     .orderBy(df['SrcAirport'], df['AvgDepDelay'])
     .write.mode("overwrite").saveAsTable("g2_q1"))


query = carriersByDepDelay \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(for_each_batch) \
    .start()

query.awaitTermination()

spark.sql("REFRESH TABLE g2_q1")
spark.sql("select * from g2_q1").show()