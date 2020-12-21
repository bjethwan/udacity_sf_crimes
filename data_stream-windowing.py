import logging
import json
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

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    service_table.printSchema()

    # # TODO select original_crime_type_name and disposition
    distinct_table = service_table

    # agg_df = distinct_table.select('call_date_time','original_crime_type_name','disposition') \
    #                         .withWatermark('call_date_time', "60 minutes") \
    #                         .groupBy(
    #                                     psf.window('call_date_time', "10 minutes", "5 minutes" ),
    #                                     psf.col('original_crime_type_name')) \
    #                         .count()
    agg_df = distinct_table.select('call_date_time','original_crime_type_name') \
                            .withWatermark('call_date_time', "1 day") \
                            .groupBy(
                                        psf.window('call_date_time', "1 day", "1 hour"),
                                        psf.col('original_crime_type_name')) \
                            .count() \
                            .orderBy('window.start', 'count')

    query = agg_df \
                .writeStream \
                .outputMode("complete") \
                .format("console") \
                .option("truncate", "false") \
                .start()
    query.awaitTermination()


    # # count the number of original crime type
    # agg_df = distinct_table \
    #     .dropna() \
    #     .select("original_crime_type_name") \
    #     .withWatermark("call_datetime", "60 minutes") \
    #     .groupby("original_crime_type_name") \
    #     .agg({"original_crime_type_name" : "count"}) \
    #     c



    # # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # # TODO write output stream
    # query = agg_df.writeStream\
    #                   .outputMode("complete")\
    #                   .format("console")\
    #                   .option("truncate", "false")\
    #                   .start()


    # # TODO attach a ProgressReporter
    # query.awaitTermination()

    # print('=== radio_code....')

    # # get the right radio code json path
    # radio_code_json_filepath = "data/radio_code.json"
    # radio_code_df = spark.read.option("multiline","true").json(radio_code_json_filepath)

    # # # # rename disposition_code column to disposition
    # radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    # rc_df = service_table.join(radio_code_df, 'disposition', 'left_outer')
    # select_rc_df = rc_df.select('call_date_time','original_crime_type_name','description').withColumnRenamed("description", "disposition")

    # # # # join on disposition column
    # radio_code_df.show(10)
    # query = select_rc_df.writeStream\
    #                   .outputMode("append")\
    #                   .format("console")\
    #                   .option("truncate", "false")\
    #                   .start().awaitTermination()


 


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

    
