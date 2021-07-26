#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

spark = SparkSession.builder \
    .appName("ccc_task2_g3q2") \
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

df_aviation_g3q2 = df_aviation.select('Year', 'Month', 'DayOfMonth', 'FlightDate', 'UniqueCarrier',
                                      'FlightNum', 'Origin', 'Dest', 'CRSDepTime', 'ArrDelay', 'Cancelled')

(df_aviation_g3q2.filter(df_aviation_g3q2["Year"] == 2008)
 .filter(df_aviation_g3q2['ArrDelay'].isNotNull())
 .createOrReplaceTempView("aviation"))

df1 = spark.sql("select concat(lpad(DayOfMonth, 2, '0'), '/', lpad(Month, 2, '0'),'/', Year) as StartDate, \
                 Origin, Dest, UniqueCarrier, FlightNum, FlightDate, CRSDepTime, ArrDelay \
                 from aviation where CRSDepTime < 1200")

df2 = spark.sql("select concat(lpad(DayOfMonth, 2, '0'), '/', lpad(Month, 2, '0'),'/', Year) as StartDate, \
                 Origin, Dest, UniqueCarrier, FlightNum, FlightDate, CRSDepTime, ArrDelay \
                 from aviation where CRSDepTime > 1200")

def for_each_batch_df1(df, epoch_id):
    window = Window.partitionBy(df['Origin'], df['Dest'], df['StartDate']).orderBy(df['ArrDelay'])
    ranked_df = df.select('*', rank().over(window).alias('rank'))
    rs = (ranked_df.filter(col('rank') == 1).select(df['*']))
    rs.write.mode("append").saveAsTable("g3_q2_a")

def for_each_batch_df2(df, epoch_id):
    window = Window.partitionBy(df['Origin'], df['Dest'], df['StartDate']).orderBy(df['ArrDelay'])
    ranked_df = df.select('*', rank().over(window).alias('rank'))
    rs = (ranked_df.filter(col('rank') == 1).select(df['*']))
    rs.write.mode("append").saveAsTable("g3_q2_b")

query1 = df1 \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(for_each_batch_df1) \
    .start()

query2 = df2 \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(for_each_batch_df2) \
    .start()

query1.awaitTermination()
query2.awaitTermination()

spark.sql("CREATE TABLE IF NOT EXISTS g3_q2 USING hive as select concat(a.Origin, '-', a.Dest, '-', b.Dest) as XYZ, \
a.StartDate, \
(a.ArrDelay + b.ArrDelay) as TotalArrDelay, \
a.Origin as FirstLeg_Origin, \
a.Dest as FirstLeg_Destn, \
concat(a.UniqueCarrier, ' ', a.FlightNum) as FirstLeg_AirlineFlightNum, \
concat(a.FlightDate, ' ', a.CRSDepTime) as FirstLeg_SchedDepart, \
a.ArrDelay as FirstLeg_ArrDelay, \
b.Origin as SecondLeg_Origin, \
b.Dest as SecondLeg_Destn, \
concat(b.UniqueCarrier, ' ', b.FlightNum) as SecondLeg_AirlineFlightNum, \
concat(b.FlightDate, ' ', b.CRSDepTime) as SecondLeg_SchedDepart, \
b.ArrDelay as SecondLeg_ArrDelay \
from \
(select StartDate, Origin, Dest, UniqueCarrier, FlightNum, FlightDate, CRSDepTime, ArrDelay, \
 row_number() over (partition by Origin, Dest, StartDate order by ArrDelay) as Seqnum \
 from g3_q2_a) a \
join \
(select StartDate, Origin, Dest, UniqueCarrier, FlightNum, FlightDate, CRSDepTime, ArrDelay, \
 row_number() over (partition by Origin, Dest, StartDate order by ArrDelay) as Seqnum \
 from g3_q2_b) b \
on a.Dest = b.Origin and date_add(a.FlightDate, 2) = b.FlightDate and a.Seqnum = 1 and b.Seqnum = 1 ")
