from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from vnstock import *
from sql_query import vm_pubsub_3M_query,vm_pubsub_1Y_query
# Create a Spark session
start_date=last_xd(365)
current_date=today()
print(start_date)
print(current_date)

spark = (
        SparkSession
        .builder
        .appName("ReadFromGCS")
        .config("spark.jars", "https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar,https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar")
        .getOrCreate()
)
# Define the GCS path to your CSV file
gcs_path = f"gs://vnstock-storage/stock-index-storage_1Y/stock_data_1Y.csv"
# Read the CSV file from GCS into a DataFrame
schema_stock_info=(
        StructType
        (
                [
                        StructField("time"                      , TimestampType()   , True),
                        StructField("open"                      , DoubleType()   , True),
                        StructField("high"                      , DoubleType()   , True),
                        StructField("low"                       , DoubleType()   , True),
                        StructField("close"                     , DoubleType()   , True),
                        StructField("volume"                    , DoubleType()   , True),
                        StructField("ticker"                    , StringType()   , True),
                        StructField("type"                      , StringType()   , True),
                        
                ]
        )
)
stock_info=(
        spark
                .read
                .option('header','true')
                .schema(schema_stock_info)
                .csv(gcs_path)
)
stock_info.createOrReplaceTempView("stock_info")

def vm_pubsub_3M():
        max_date = spark.sql("SELECT max(time) as max_date FROM stock_info").first()["max_date"]
        row_index= 10
        limit_max = 10
        limit_min = 8
        print(max_date)
        print(type(max_date))
        result = spark.sql(
                vm_pubsub_3M_query(max_date,row_index,limit_max,limit_min)
        )
        result.show
        result.printSchema()

        result.write.format("bigquery") \
                        .option("credentialsFile","/home/nguyentuanduong7/airflow/key/credentials.json") \
                        .option("temporaryGcsBucket","gs://vnstock-storage/") \
                        .option("table", "vnstock-pipeline.vnstock_data.vnstock_3M") \
                        .mode('overwrite') \
                        .save()
        print("DONE_spark_1")
def vm_pubsub_1Y():
        result = spark.sql(
                vm_pubsub_1Y_query()
        )
        result.show()
        result.printSchema()

        result.write.format("bigquery") \
                        .option("credentialsFile","/home/nguyentuanduong7/airflow/key/credentials.json") \
                        .option("temporaryGcsBucket","gs://vnstock-storage/") \
                        .option("table", "vnstock-pipeline.vnstock_data.vnstock_1Y") \
                        .mode('overwrite') \
                        .save()

        print("DONE_spark_1Y")


vm_pubsub_1Y()
