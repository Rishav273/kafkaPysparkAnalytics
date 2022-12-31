from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, LongType, DateType, FloatType, TimestampType
import yaml
import json
import os

with open('config.yaml', 'r') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# SPARK SPECIFIC FUNCTIONS

def read_events_from_kafka(spark, topic, starting_offset='latest'):

    """
    Read events from a kafka topic into a spark streaming dataframe and return it.

    spark: Existing sparksession object.
    topic: kafka Topic to read events from.
    starting_offset: Can be either of ['latest', 'earliest]. Default: 'latest'
    """

    df = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', topic) \
        .option('startingOffsets', starting_offset) \
        .load()

    return df

def write_streaming_df_to_file_sink(spark, df, checkpoint_string, path, outputMode, truncate='false', format='console'):

    """
    Write spark streaming df to console or a file sink.

    spark: Existing sparksession object.
    df: Spark streaming dataframe.
    checkpoint: Checkpoint location to be used.
    path: output path (if required).
    outputMode: Can be either of ['append', 'complete', 'update'].
    truncate: Truncate messages (default 'true').
    format: Cane be either of ['console', 'csv', 'json', 'orc', 'parquet']. Default: 'console'
    """

    query = df.writeStream \
    .format(format) \
    .option('checkpointLocation', f"{config['CHECKPOINT_LOC']}/{checkpoint_string}") \
    .outputMode(outputMode) \
    .option('truncate', truncate) \
    .trigger(processingTime='1 minute') \
    .option('path', f"{config['OUTPUT_PATH']}/{path}") \
    .start()

    return query


def process_raw_df_from_kafka_source(df, schema):

    """
    Take a streaming dataframe from kafka source and return a processed streaming df with
    appropriate schema.

    spark: Existing SparkSession.
    df: Input Dataframe.
    schema: Explicit schema definition.
    """
    base_df = df.selectExpr("CAST(value as STRING) as value", 'timestamp')
    
    # base_df.printSchema()

    processed_df = base_df.select(
        from_json(col('value'), schema).alias('value'), 'timestamp'
    )

    # processed_df.printSchema()

    explode_df = processed_df.selectExpr(
        'value.ssn',
        'value.cc_num',
        'CAST(value.is_fraud as INT) as is_fraud',
        'value.trans_num',
        'value.category',
        'CAST(value.amt AS FLOAT) as amt',
        'value.merchant',
        'timestamp'
    )

    return explode_df

def define_schema():
    schema = StructType(
        [
        StructField('ssn', StringType(),True),
        StructField('cc_num', StringType(),True),
        StructField('first', StringType(),True),
        StructField('last', StringType(),True),
        StructField('gender', StringType(),True),
        StructField('street', StringType(),True),
        StructField('city', StringType(),True),
        StructField('state', StringType(),True),
        StructField('zip', StringType(),True),
        StructField('lat', StringType(),True),
        StructField('long', StringType(),True),
        StructField('city_pop', StringType(),True),
        StructField('job', StringType(),True),
        StructField('dob', StringType(),True),
        StructField('acct_num', StringType(),True),
        StructField('profile', StringType(),True),
        StructField('trans_num', StringType(),True),
        StructField('trans_date', StringType(),True),
        StructField('trans_time', StringType(),True),
        StructField('unix_time', StringType(),True),
        StructField('category', StringType(),True),
        StructField('amt', StringType(),True),
        StructField('is_fraud', StringType(),True),
        StructField('merchant', StringType(),True),
        StructField('merch_lat', StringType(),True),
        StructField('merch_long', StringType(),True)
        ]
    )

    return schema


# KAFKA SPECIFIC FUNCTIONS
def list_files(path, excluded):
    # return a list of files to be sent to kafka topic, excluding some files.
    items = [item for item in os.listdir(path) if item not in excluded]
    return items

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


# CONFIG FUNCTION
def load_config():
    with open('config.yaml', 'r') as f:
        configuration = yaml.load(f, Loader=yaml.FullLoader)

    return configuration