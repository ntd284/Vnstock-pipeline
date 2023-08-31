from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from vnstock import *

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
        f"""    
                WITH cte_daily_amount AS     (
                        SELECT  
                                CAST(time AS DATE)  AS Time,
                                ticker,
                                SUM(close) AS price_close
                        FROM
                                stock_info
                        GROUP BY Time ,ticker

                        HAVING
                                CAST(time AS DATE) BETWEEN DATE_SUB(CAST('{str(max_date)}' AS DATE) , 90) AND CAST('{str(max_date)}' AS DATE)
                                )
                SELECT  Time,
                        ticker,
                        price_close,
                        previous_date_price,
                        percentage_change_close,
                        MA10,
                        percentage_change_MA10,
                        sum_percentage_change_close,
                        sum_percentage_change_MA10
                FROM(
                        
                SELECT  Time,
                        ticker,
                        price_close,
                        previous_date_price,
                        percentage_change_close,
                        MA10,
                        percentage_change_MA10,
                        ROUND((SUM(percentage_change_close) OVER (PARTITION BY ticker)),2) AS sum_percentage_change_close,
                        ROUND((SUM(percentage_change_MA10) OVER (PARTITION BY ticker)),2) AS sum_percentage_change_MA10

                FROM(
                SELECT    Time,ticker,price_close,MA10,percentage_change_MA10,
                        MAX(percentage_change_MA10) OVER (PARTITION BY ticker) AS max_percentage_change,
                        previous_date_price,
                        ROUND(((price_close-previous_date_price)*100/previous_date_price),2) AS percentage_change_close,

                        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time) AS row_num
                FROM (
                SELECT  Time,
                        ticker,
                        price_close,
                        CASE 
                                WHEN ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY Time) > {row_index} 
                                THEN ROUND(AVG(price_close) OVER(PARTITION BY ticker 
                                        ORDER BY Time ROWS BETWEEN {row_index} PRECEDING AND CURRENT ROW),2)
                                ELSE NULL 
                        END AS MA10,
                        LAG(price_close, -1) OVER (PARTITION BY ticker ORDER BY time DESC) AS previous_date_price,

                        ROUND(((price_close-MA10)*100/MA10),2) AS percentage_change_MA10
                FROM
                        cte_daily_amount  )
                AS percentage_change_MA10
                ) AS max_percentage_change
                WHERE max_percentage_change <= {limit_max} AND max_percentage_change >= {limit_min} AND row_num > 10
                ) AS sum_percentage_change_close
                WHERE sum_percentage_change_close > 0
                ;
        """
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
        f"""SELECT      time,
                        ticker,
                        type,
                        close,
                        open,
                        high,
                        low,
                        volume,
                        ROUND(((close-previous_date_price)*100/previous_date_price),2) AS percentage_change
                        FROM 
                        (
                        SELECT
                        time,
                        type,
                        ticker,
                        close,
                        open,
                        high,
                        low,
                        volume,
                        LAG(close,-1) OVER (
                                PARTITION BY ticker
                                ORDER BY time DESC
                        ) AS previous_date_price
                        FROM stock_info
                        ) AS result
                        
        """
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



