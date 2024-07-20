from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import requests
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os


def parse_key_value(json_dict, parent):
    key_value_list = []
    for key, value in json_dict.items():
        if isinstance(value, dict):
            if parent == "": next_parent = key
            else: next_parent = parent + "." + key
            key_value_list.extend(parse_key_value(value, next_parent))
        else:
            if parent == "": this_key = key
            else: this_key = parent + "." + key
            key_value_list.append((this_key, str(value)))
    return key_value_list


def read_key_value(json_str):
    json_dict = json.loads(json_str.replace("\'", "\""))
    key_value_list = parse_key_value(json_dict, "")
    return key_value_list


def insert_data(table, primary_key, df, conn_param):
    columns = [f'"{col}"' for col in df.schema.names]
    data = [tuple(row) for row in df.collect()]

    try:
        conn = psycopg2.connect(**conn_param)
        cursor = conn.cursor()
        insert_query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s ON CONFLICT ({primary_key}) DO NOTHING"
        execute_values(cursor, insert_query, data)
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

def create_view(conn_param):
    sql_query = """
        CREATE OR REPLACE VIEW timeseries_data AS
        SELECT
            ti.uuid,
            ti.item_id, 
            ta.attribute, 
            tv.value
        FROM timeseries_value tv 
            JOIN timeseries_attribute ta ON tv.attribute = ta.attribute
            JOIN timeseries_item ti ON tv.item_id = ti.item_id;
    """
    try:
        conn = psycopg2.connect(**conn_param)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    spark = SparkSession.builder \
    .appName("mini-project") \
    .getOrCreate()

    api = "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json?includeTimeseries=true&hasTimeseries=WV&includeForecastTimeseries=true"
    responses = requests.get(api).json()

    timeseries_data = [(response['uuid'], response['uuid']+str(item_id), json.dumps(item)) 
                for response in responses
                for item_id, item in enumerate(response['timeseries'])]
    [item.pop('timeseries') for item in responses]
    response_str = json.dumps(responses)

    # Create a dataframe from frequent fields in the response
    response_df = spark.read.json(spark.sparkContext.parallelize([response_str]))
    response_df = response_df.select(
        col("agency"),
        col("km").cast("double"),
        col("latitude").cast("double"),
        col("longitude").cast("double"),
        col("longname"),
        col("number"),
        col("shortname"),
        col("uuid"),
        col("water.longname").alias("water_longname"),
        col("water.shortname").alias("water_shortname")
    )
    
    # Create a dataframe from timeseries data
    timeseries_df = spark.createDataFrame(timeseries_data, ['uuid', 'item_id', 'timeseries_data'])

    schema = ArrayType(StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True)
    ]))
    explode_udf = udf(read_key_value, schema)

    # Convert the nested dictionary to list of key-value pairs corresponding to the dictionary
    timeseries_parsed_df = timeseries_df.withColumn("timeseries_parsed", explode_udf(col('timeseries_data')))
    timeseries_parsed_df = timeseries_parsed_df.withColumn("exploded_pair", explode(col('timeseries_parsed')))
    timeseries_parsed_df = timeseries_parsed_df.withColumn("key", col("exploded_pair").getItem("key")) \
                    .withColumn("value", col("exploded_pair").getItem("value"))
    timeseries_parsed_df = timeseries_parsed_df.drop("timeseries_data", "exploded_pair","timeseries_parsed")

    timeseries_item = timeseries_df.select(col("item_id"),col("uuid"))

    timeseries_attribute_df = timeseries_parsed_df.select(col("key").alias("attribute")).dropDuplicates()

    timeseries_attr_val_df = timeseries_parsed_df.select(col("item_id"), col("key").alias("attribute"), col("value"))

    load_dotenv()
    conn_params = {
        "dbname": os.getenv('dbname'),
        "user": os.getenv('user'),
        "password": os.getenv('password'),
        "host": os.getenv('host'),
        "port": os.getenv('port')
    }

    insert_data("responses", "uuid", response_df, conn_params)
    insert_data("timeseries_item", "item_id", timeseries_item, conn_params)
    insert_data("timeseries_attribute", "attribute", timeseries_attribute_df, conn_params)
    insert_data("timeseries_value", "id", timeseries_attr_val_df, conn_params)
    create_view(conn_params)
    spark.stop()

