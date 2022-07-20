from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr, lit
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, DoubleType


redis_server_topic_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", DoubleType())
            ])
        )),
    ]
)

customer_schema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

customer_risk_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", DoubleType()),
        StructField("riskDate", DateType())
    ]
)

spark = SparkSession.builder.appName("customer-data").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redis_server_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

redis_server_stream = redis_server_stream.selectExpr(
    "cast(value as string) value")

column_names = ('value.key', 'value.existType', 'value.Ch',
                'value.Incr', 'value.zSetEntries')

redis_server_stream.withColumn("value", from_json("value", redis_server_topic_schema)) \
    .select(*column_names) \
    .withColumnRenamed('Ch', 'ch') \
    .withColumnRenamed('Incr', 'incr') \
    .withColumn('value', lit(None).cast(StringType())) \
    .withColumn('expiredType', lit(None).cast(StringType())) \
    .withColumn('expiredValue', lit(None).cast(StringType())) \
    .createOrReplaceTempView("RedisSortedSet")

encoded_customer_stream = spark.sql(
    """
    select 
        key, 
        zSetEntries[0].element as encodedCustomer 
    from RedisSortedSet
    """
)

encoded_customer_stream_decoded = encoded_customer_stream \
    .withColumn("customer", unbase64(encoded_customer_stream.encodedCustomer).cast("string"))

encoded_customer_stream_decoded \
    .withColumn("customer", from_json("customer", customer_schema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql(
    """
    select 
        email,
        birthDay 
    from CustomerRecords 
    where email is not null 
        and birthDay is not null
    """
)

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .select('email', split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0).alias("birthYear"))

stedi_events_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

stedi_events_stream = stedi_events_stream.selectExpr(
    "cast(value as string) value")

stedi_events_stream \
    .withColumn("value", from_json("value", customer_risk_schema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("""
    select 
        customer, 
        score 
    from CustomerRisk
"""
                                    )

riskScoreByBirthYear = customerRiskStreamingDF.join(emailAndBirthYearStreamingDF, expr("""
    customer = email
"""
                                                                                       ))

riskScoreByBirthYear.writeStream.outputMode(
    "append").format("console").start().awaitTermination()

riskScoreByBirthYear.selectExpr(
    "cast(customer as string) as key",
    "to_json(struct(*)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("topic", "customer-risk") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
