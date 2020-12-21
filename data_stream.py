import logging
import json
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# export JAVA_HOME=`/usr/libexec/java_home -v 1.8.0_251`
# unset PYSPARK_DRIVER_PYTHON
# python3 producer_server.py
# python3 kafka_server.py
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 data_stream.py

# TODO Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('disposition', StringType(), True),
    StructField('report_date', TimestampType(), True),
    StructField('call_date', TimestampType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', TimestampType(), True),
    StructField('offense_date', TimestampType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])

# This is unnecessary as we have call_date_time in the data, maybe an extra course requirement
def get_timestamp(date, time):
    date_string = str(date)
    if date_string.endswith(' 00:00:00'):
        date_string = date_string[:-9]
    datetime_string = date_string + " " + str(time)
    date_time = datetime.datetime.strptime(datetime_string, "%Y-%m-%d %H:%M")
    return date_time

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark.readStream \
            .format("kafka")\
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "bipin.udacity.police.dept.service.calls") \
            .option("startingOffsets", "earliest") \
            .option("maxRatePerPartition", 100) \
            .option("maxOffsetPerTrigger", 100) \
            .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    add_datetime_udf = psf.udf(get_timestamp, TimestampType())

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*") \
        .withColumn('udf_call_date_time', add_datetime_udf("call_date", "call_time"))



    # # TODO select original_crime_type_name and disposition
    #distinct_table = service_table.select('call_date_time','original_crime_type_name','disposition')
    distinct_table = service_table.select('udf_call_date_time','original_crime_type_name','disposition')

    # count the number of original crime type
    # agg_df = distinct_table.select('call_date_time','original_crime_type_name','disposition') \
    #                         .withWatermark('call_date_time', "3 hour") \
    #                         .groupBy(
    #                                     psf.window('call_date_time', "1 hour", "30 minutes"), 
    #                                     psf.col('original_crime_type_name'), 
    #                                     psf.col('disposition')) \
    #                         .count() \
    #                         .orderBy('window.start', 'count')

    # count the number of original crime type
    # InternalKafkaConsumer: CachedKafkaConsumer is not running in UninterruptibleThread. It may hang when CachedKafkaConsumer's methods are interrupted because of KAFKA-1894
    agg_df = distinct_table.select('udf_call_date_time','original_crime_type_name','disposition') \
                            .withWatermark('udf_call_date_time', "3 hour") \
                            .groupBy(
                                        psf.window('udf_call_date_time', "1 hour", "30 minutes"), 
                                        psf.col('original_crime_type_name'), 
                                        psf.col('disposition')) \
                            .count() \
                            .orderBy('window.start', 'count')


    # # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # # TODO write output stream
    # query = agg_df \
    #             .writeStream \
    #             .outputMode("update") \
    #             .format("console") \
    #             .option("truncate", "false") \
    #             .start()

    # # # TODO attach a ProgressReporter
    # query.awaitTermination()


    print('=== radio_code....')

    # get the right radio code json path
    radio_code_json_filepath = "../data/radio_code.json"
    radio_code_df = spark.read.option("multiline","true").json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # # # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    join_query = agg_df.join(radio_code_df, 'disposition', 'left_outer') \
                        .select('window','original_crime_type_name','description','count') \
                        .withColumnRenamed("description", "disposition") \
                        .writeStream \
                        .outputMode("complete") \
                        .format("console") \
                        .option("truncate", "false") \
                        .start()

    join_query.awaitTermination()
 


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
       .builder \
       .master("local[*]") \
       .appName("KafkaSparkStructuredStreaming") \
       .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    logger.info("Spark started")

    run_spark_job(spark)
    spark.stop()

    
