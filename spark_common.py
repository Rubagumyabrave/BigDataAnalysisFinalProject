import os
from pyspark.sql import SparkSession, types as T

DATA = os.environ.get("DATA_DIR", "/home/jovyan/work/data")
PARQUET = os.environ.get("PARQUET_DIR", "/home/jovyan/work/parquet")

def get_spark(app="ecommerce"):
    spark = (SparkSession.builder.master("local[2]").appName(app)
             .config("spark.driver.memory", "2g")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark

S = T.StringType()
geo = T.StructType([T.StructField(n, S) for n in ["city", "state", "country", "ip_address"]])
device = T.StructType([T.StructField(n, S) for n in ["type", "os", "browser"]])
page_view = T.StructType([T.StructField("timestamp", S), T.StructField("page_type", S),
                          T.StructField("product_id", S), T.StructField("category_id", S),
                          T.StructField("view_duration", T.LongType())])
cart = T.MapType(S, T.StructType([T.StructField("quantity", T.LongType()),
                                  T.StructField("price", T.DoubleType())]))

# Explicit schemas: Spark skips the extra pass over the 188 MB files that inference would need.
SESSIONS_SCHEMA = T.StructType([
    T.StructField("session_id", S), T.StructField("user_id", S),
    T.StructField("start_time", S), T.StructField("end_time", S),
    T.StructField("duration_seconds", T.LongType()), T.StructField("geo_data", geo),
    T.StructField("device_profile", device), T.StructField("viewed_products", T.ArrayType(S)),
    T.StructField("page_views", T.ArrayType(page_view)), T.StructField("cart_contents", cart),
    T.StructField("conversion_status", S), T.StructField("referrer", S)])
