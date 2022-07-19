from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType

customer_risk_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", DoubleType()),
        StructField("riskDate", DateType())
    ]
)
 
spark = SparkSession.builder.appName("customer-risk-score").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

stedi_events_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

stedi_events_stream = stedi_events_stream.selectExpr("cast(value as string) value")

stedi_events_stream \
    .withColumn("value", from_json("value", customer_risk_schema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("CustomerRisk")


customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

customerRiskStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination() 