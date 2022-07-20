from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

kafkaRedisSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("Score", StringType())
            ]))
        )
    ]
)

kafkaCustomerJSONSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("email", StringType()),
        StructField("birthYear", StringType())
    ]
)

kafkaStediEventSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

spark = SparkSession.builder.appName("steadi-redis").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

kafkaRedisServerDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("subscribe", "redis-server")\
    .option("startingOffsets", "earliest")\
    .load()

kafkaRedisServerDF = kafkaRedisServerDF.selectExpr(
    "cast(value as string) value")


kafkaRedisServerDF.withColumn("value", from_json("value", kafkaRedisSchema))\
    .select(col('value.existType'), col('value.Ch'),
            col('value.Incr'), col('value.zSetEntries'))\
    .createOrReplaceTempView("RedisSortedSet")

encodedCustomerDF = spark.sql(
    "SELECT zSetEntries[0].element as encodedCustomer FROM RedisSortedSet")

DecodedCustomerDF = encodedCustomerDF.withColumn(
    "customer", unbase64(encodedCustomerDF.encodedCustomer).cast("string"))

customerJSONschema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

DecodedCustomerDF.withColumn("customer", from_json("customer", customerJSONschema)) \
    .select(col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql(
    "select * from CustomerRecords WHERE email is not null AND birthDay is not null")

emailAndBirthDayStreamingDF = emailAndBirthDayStreamingDF.withColumn(
    'birthYear', split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0))

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(
    col('email'), col('birthYear'))

emailAndBirthYearStreamingDF = emailAndBirthYearStreamingDF\
    .select(col("email"), col("birthYear"))

emailAndBirthYearStreamingDF.writeStream.outputMode(
    "append").format("console").start().awaitTermination()
