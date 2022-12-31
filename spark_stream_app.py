# importing spark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, count, desc, window
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DateType, FloatType, TimestampType
from lib.utils import read_events_from_kafka, write_streaming_df_to_file_sink, process_raw_df_from_kafka_source, define_schema, load_config
import yaml

# read config file
config = load_config()

# importing logging modules
import logging
from lib.logger import Logger

# define explicit schema
schema = define_schema()


if __name__ == "__main__":

    # create logger instance
    logger = Logger(
        'SparkStreamingApp',
        'application_logs/log.txt',
        level=logging.INFO
    )

    logger.info('Starting SparkSession...')
    # create a spark session for handling transactions data
    spark = SparkSession.builder.master('local[3]').appName('transactionsApp').getOrCreate()

    # stream from source to an input df
    df = read_events_from_kafka(
        spark,
        topic='transactions',
        starting_offset='latest'
    )

    logger.info('Displaying raw streaming dataframe schema...')
    df.printSchema()

    # process the raw dataframe
    processed_df = process_raw_df_from_kafka_source(df, schema)

    logger.info('Displaying raw streaming dataframe schema...')
    processed_df.printSchema()

    # total fraud transactions per transaction category
    fraud_transactions_per_category_df = processed_df \
        .withWatermark('timestamp', '10 seconds') \
        .groupBy(
        window(col('timestamp'), '10 seconds'),
        col('category')
    ).agg(
        sum('is_fraud').alias('total_frauds')
    ).select(
        'window.start',
        'window.end',
        'category',
        'total_frauds'
    )
    
    logger.info('Displaying total frauds per category schema...')
    fraud_transactions_per_category_df.printSchema()

    # write df to sink
    query1 = write_streaming_df_to_file_sink(spark, 
                fraud_transactions_per_category_df, 
                checkpoint_string='frauds-per-category', 
                path='frauds-per-category', 
                outputMode='update', 
                truncate='false', 
                format='console')

    #############################################################################

    # total frauds per window       
    frauds_per_window = processed_df \
            .withWatermark('timestamp', '10 seconds') \
            .groupBy(window(col('timestamp'), '10 seconds')) \
            .agg(sum('is_fraud').alias('total_frauds')) \
            .select(
                'window.start', 'window.end', 'total_frauds'
            )

    logger.info('Displaying total frauds per window schema...')
    frauds_per_window.printSchema()

   # write df to sink
    query2 = write_streaming_df_to_file_sink(spark, 
                frauds_per_window, 
                checkpoint_string='frauds-per-window', 
                path='frauds-per-window', 
                outputMode='update', 
                truncate='false', 
                format='console')

    # #############################################################################

    #frauds per merchant
    frauds_per_merchant = processed_df \
        .withWatermark('timestamp', '10 seconds') \
        .groupBy(window(col('timestamp'), '10 seconds'), 'merchant') \
        .agg(
            sum('is_fraud').alias('total_frauds')
        ).select(
            'window.start', 'window.end', 'merchant', 'total_frauds'
        )

    logger.info('Displaying total frauds per merchant schema...')
    frauds_per_merchant.printSchema()

   # write df to sink
    query3 = write_streaming_df_to_file_sink(spark, 
                frauds_per_merchant, 
                checkpoint_string='frauds-per-merchant', 
                path='frauds-per-merchant', 
                outputMode='update', 
                truncate='false', 
                format='console')

   # Terminate streaming when any one of the jobs fail
    logger.info('Starting stream...')
    spark.streams.awaitAnyTermination()

    #############################################################################

    

    

    


    
