from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, month, year, count, sum as spark_sum, sha2, when, concat,lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from influxdb_client import InfluxDBClient, Point, WritePrecision
import requests
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import paramiko
# Set up checkpoint directory (ensure this path exists or can be created in your environment)
checkpoint_dir = "//home/maf/StreamingPipeline/StreamingPipeline/checkpoint"
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB configuration
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "32xn55CeJleTx74AflUjqhMYAN_Ko6VsKlqayFz-uE-IVqiMFYN3VsjcrUODm9MDclM_s9v4bXkaENsYaVt5Yg=="
INFLUXDB_ORG = "NA"
INFLUXDB_BUCKET = "sales_data"

# Initialize Spark session
spark = SparkSession.builder.appName("RealTimeDataPipeline").getOrCreate()
spark.sparkContext.setLogLevel("INFO")

# Define schemas for each data source
gps_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("CarID", StringType(), True),
    StructField("OfficeID", StringType(), True),
    StructField("AgentID", StringType(), True),
    StructField("TRXN_Timestamp", TimestampType(), True),
    StructField("CarDrivingStatus", StringType(), True),
    StructField("CurrentLongitude", DoubleType(), True),
    StructField("CurrentLatitude", DoubleType(), True),
    StructField("CurrentArea", StringType(), True),
    StructField("KM", DoubleType(), True)
])

sales_schema = StructType([
    StructField("AgentID", StringType(), True),
    StructField("OfficeID", StringType(), True),
    StructField("CarID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("Amount", DoubleType(), True)
])

customer_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("MobileNo", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Nationality", StringType(), True),
    StructField("PassportNo", StringType(), True),
    StructField("IDNo", StringType(), True),
    StructField("HomeAddress", StringType(), True),
    StructField("LeaseStartDate", TimestampType(), True),
    StructField("LeasePeriod", IntegerType(), True)
])

car_schema = StructType([
    StructField("CarID", StringType(), True),
    StructField("CarMake", StringType(), True),
    StructField("CarModel", StringType(), True),
    StructField("PlateNo", StringType(), True),
    StructField("RegistrationDate", TimestampType(), True),
    StructField("RegistrationExpiryDate", TimestampType(), True)
])

office_schema = StructType([
    StructField("OfficeID", StringType(), True),
    StructField("MobileNo", StringType(), True),
    StructField("Area", StringType(), True),
    StructField("OfficeNo", StringType(), True),
    StructField("WorkingHours", StringType(), True)
])

agent_schema = StructType([
    StructField("AgentID", StringType(), True),
    StructField("MobileNo", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Nationality", StringType(), True),
    StructField("OfficeID", StringType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SalesDataPipeline") \
    .getOrCreate()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def write_to_influx(measurement, data):
    try:
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            write_api = client.write_api(write_options=WritePrecision.NS)
            point = Point(measurement).field("value", data)
            write_api.write(INFLUXDB_BUCKET, INFLUXDB_ORG, point)
    except Exception as e:
        print(f"Failed to write to InfluxDB: {e}")

def create_streaming_pipeline():
    # Define Kafka sources
    kafka_options = {
        "kafka.bootstrap.servers": "localhost:9092",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }

    # Read data streams from Kafka topics
    gps_data = spark.readStream.format("kafka") \
        .option("subscribe", "gps_data") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load() \
        .select(from_json(col("value").cast("string"), gps_schema).alias("data")).select("data.*")

    sales_data = spark.readStream.format("kafka") \
        .option("subscribe", "sales_data") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load() \
        .select(from_json(col("value").cast("string"), sales_schema).alias("data")).select("data.*")

    customer_data = spark.readStream.format("kafka") \
        .option("subscribe", "customer_data") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load() \
        .select(from_json(col("value").cast("string"), customer_schema).alias("data")).select("data.*")

    car_data = spark.readStream.format("kafka") \
        .option("subscribe", "car_data") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load() \
        .select(from_json(col("value").cast("string"), car_schema).alias("data")).select("data.*")

    office_data = spark.readStream.format("kafka") \
        .option("subscribe", "office_data") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load() \
        .select(from_json(col("value").cast("string"), office_schema).alias("data")).select("data.*")

    agent_data = spark.readStream.format("kafka") \
        .option("subscribe", "agent_data") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load() \
        .select(from_json(col("value").cast("string"), agent_schema).alias("data")).select("data.*")
    
# GPS Data Quality Checks (focusing on essential columns)
    gps_data = gps_data.filter(
        (col("CurrentLongitude").isNotNull()) &  # Longitude must be present
        (col("CurrentLatitude").isNotNull()) &  # Latitude must be present
        (col("CarDrivingStatus").isin("Stopped", "Idle", "Moving"))  # CarDrivingStatus should be one of the allowed values
    )

# Sales Data Quality Checks (focusing on essential columns)
    sales_data = sales_data.filter(
        (col("Amount") > 0) &  # Amount must be positive
        col("AgentID").isNotNull() &  # AgentID must be present
        col("CustomerID").isNotNull()  # CustomerID must be present
    )

# Customer Data Quality Checks (focusing on essential columns)
    customer_data = customer_data.filter(
        col("CustomerID").isNotNull() &  # CustomerID must be present
        col("MobileNo").isNotNull() &  # MobileNo must be present
        (col("Age") > 0) & (col("Age") <= 120)  # Age must be within a reasonable range
    )

# Car Data Quality Checks (focusing on essential columns)
    car_data = car_data.filter(
        col("CarID").isNotNull() &  # CarID must be present
        col("PlateNo").isNotNull()  # PlateNo must be present
    )

# Office Data Quality Checks (focusing on essential columns)
    office_data = office_data.filter(
        col("OfficeID").isNotNull() &  # OfficeID must be present
        col("Area").isNotNull()  # Area must be present
    )

# Agent Data Quality Checks (focusing on essential columns)
    agent_data = agent_data.filter(
        col("AgentID").isNotNull() &  # AgentID must be present
        col("OfficeID").isNotNull()  # OfficeID must be present
)

    # Define enriched_data by joining all data sources and including all columns from each source
    # Step 1: Hash mobile numbers and create an age band for customer and agent data
    customer_data = customer_data.withColumn("HashedMobileNo", sha2(col("MobileNo"), 256))
    agent_data = agent_data.withColumn("AgeBand", 
                                    when(col("Age") < 18, "<18")
                                    .when((col("Age") >= 18) & (col("Age") <= 30), "19-30")
                                    .when((col("Age") >= 31) & (col("Age") <= 50), "31-50")
                                    .otherwise("51+"))
    car_data = car_data.withColumn("HashedPlateNo", sha2(col("PlateNo"), 256))

    # Select only the anonymized columns for sensitive data
    anonymized_data = customer_data.select("CustomerID", "HashedMobileNo", "PassportNo", "IDNo") \
        .join(car_data.select("CarID", "HashedPlateNo"), "CarID", "left")

    non_anonymized_data = gps_data \
        .join(sales_data, ["CustomerID", "CarID", "OfficeID", "AgentID"], "left") \
        .join(customer_data.select("CustomerID", "Name", "Gender", "Age", "Nationality", "HomeAddress", "LeaseStartDate", "LeasePeriod"), "CustomerID", "left") \
        .join(car_data.select("CarID", "CarMake", "CarModel", "RegistrationDate", "RegistrationExpiryDate"), "CarID", "left") \
        .join(office_data.select("OfficeID", "MobileNo", "Area", "OfficeNo", "WorkingHours"), "OfficeID", "left") \
        .join(agent_data.select("AgentID", "MobileNo", "Name", "Gender", "Age", "Nationality", "OfficeID"), "AgentID", "left") \
        .filter(col("CurrentArea") == "Dubai")

    base_path = "/home/maf/StreamingPipeline/StreamingPipeline/anonymized_and_nonanonmyized"
    anonymized_path = f"{base_path}/anonymized_data"
    non_anonymized_path = f"{base_path}/non_anonymized_data"

    # Write anonymized data to the specified path with append mode
    anonymized_data.write.mode("append").parquet(anonymized_path)

    # Write non-anonymized data to the specified path with append mode
    non_anonymized_data.write.mode("append").parquet(non_anonymized_path)

    print(f"Anonymized data saved to: {anonymized_path}")
    print(f"Non-anonymized data saved to: {non_anonymized_path}")

    

    # Define aggregations with stateful operations
    # 1. Count of sales per month and year
    sales_count = sales_data.withWatermark("TRXN_Timestamp", "1 month") \
        .groupBy(year("TRXN_Timestamp").alias("Year"), month("TRXN_Timestamp").alias("Month")) \
        .agg(count("*").alias("SalesCount"))

    # 2. Volume of sales per month and year
    sales_volume = sales_data.withWatermark("TRXN_Timestamp", "1 month") \
        .groupBy(year("TRXN_Timestamp").alias("Year"), month("TRXN_Timestamp").alias("Month")) \
        .agg(spark_sum("Amount").alias("SalesVolume"))

    # 3. Count of Car Driving Status per area
    car_status_count = gps_data \
        .groupBy("CurrentArea", "CarDrivingStatus") \
        .agg(count("*").alias("StatusCount"))

    # Start streams with checkpointing and complete mode for stateful operations
    sales_count_query = sales_count.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", f"{checkpoint_dir}/sales_count") \
        .start()

    sales_volume_query = sales_volume.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", f"{checkpoint_dir}/sales_volume") \
        .start()

    car_status_count_query = car_status_count.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("checkpointLocation", f"{checkpoint_dir}/car_status_count") \
        .start()

    # Write results to InfluxDB
    def write_sales_count_to_influx(batch_df, batch_id):
        for row in batch_df.collect():
            write_to_influx("sales_count", row.asDict())

    def write_sales_volume_to_influx(batch_df, batch_id):
        for row in batch_df.collect():
            write_to_influx("sales_volume", row.asDict())

    def write_car_status_count_to_influx(batch_df, batch_id):
        for row in batch_df.collect():
            write_to_influx("car_status_count", row.asDict())

    # Execute the streaming queries
    sales_count_query = sales_count.writeStream.foreachBatch(write_sales_count_to_influx).start()
    sales_volume_query = sales_volume.writeStream.foreachBatch(write_sales_volume_to_influx).start()
    car_status_query = car_status_count.writeStream.foreachBatch(write_car_status_count_to_influx).start()

    # Wait for the streaming queries to finish
    sales_count_query.awaitTermination()
    sales_volume_query.awaitTermination()
    car_status_query.awaitTermination()
    

    # Joining data into unified_output_data then passing it sftp
    unified_output_data = gps_data \
        .join(sales_data, ["CustomerID", "CarID", "OfficeID", "AgentID"], "left") \
        .join(customer_data, "CustomerID", "left") \
        .join(car_data, "CarID", "left") \
        .join(office_data, "OfficeID", "left") \
        .join(agent_data, "AgentID", "left") \
        .filter(col("CurrentArea") == "Dubai")

    # Step 3: Select and rename columns to match the desired output structure
    unified_output_data = unified_output_data.select(
        concat(lit("BATCH_"), col("CustomerID"), col("CarID"), col("AgentID")).alias("BatchID"),
        col("TRXN_Timestamp"),
        col("CarDrivingStatus"),
        col("CurrentLongitude"),
        col("CurrentLatitude"),
        col("CurrentArea"),
        col("KM"),
        col("Amount").alias("Amount (USD)"),
        col("HashedMobileNo").alias("Customer Hashed Mobile No"),
        col("Name").alias("Customer Name"),
        col("Gender").alias("Customer Gender"),
        col("Age").alias("Customer Age"),
        col("Nationality").alias("Customer Nationality"),
        col("PassportNo").alias("Customer Passport No"),
        col("IDNo").alias("Customer ID No"),
        col("HomeAddress").alias("Customer Home Address"),
        col("LeaseStartDate"),
        col("LeasePeriod"),
        col("CarMake").alias("Car Make"),
        col("CarModel").alias("Car Model"),
        col("PlateNo"),
        col("RegistrationDate"),
        col("RegistrationExpiryDate"),
        col("MobileNo").alias("Office Mobile No"),
        col("Area").alias("Office Area"),
        col("OfficeNo"),
        col("WorkingHours"),
        col("MobileNo").alias("Agent Mobile No"),
        col("Name").alias("Agent Name"),
        col("Gender").alias("Agent Gender"),
        col("AgeBand").alias("Agent Age Band"),
        col("Nationality").alias("Agent Nationality")
    )


    # real-time sync function

    def sync_to_api(row):
        url = "http://localhost:5000/sync"
        payload = row.asDict()
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code != 200:
            print(f"Failed to sync record {payload['BatchID']}: {response.status_code}")
        else:
            print(f"Successfully synced record {payload['BatchID']}")


        # save and transfer data to sFTP in batches
        # save_to_sftp to save files locally in the specified directory
    def save_to_sftp(df, epoch_id):
        # Define local directory for saving files
        local_dir = "/home/maf/StreamingPipeline/StreamingPipeline/sftp_data"
        
        # Spliting into batches of 2000 records and then further split each batch into four files of 500 records
        batches = df.randomSplit([2000] * (df.count() // 2000 + 1))
        
        for batch_idx, batch_df in enumerate(batches):
            # Split batch into 4 files of 500 records each
            sub_batches = batch_df.randomSplit([500, 500, 500, 500])
            
            for file_idx, sub_batch_df in enumerate(sub_batches):
                # Save each sub-batch to a local CSV file
                file_path = f"{local_dir}/unified_output_data_epoch_{epoch_id}_batch_{batch_idx}_file_{file_idx}.csv"
                sub_batch_df.coalesce(1).write.mode("overwrite").csv(file_path, header=True)
                
                # sFTP transfer logic using Paramiko
                try:
                    sftp_host = "sftp.example.com"
                    sftp_username = "user"
                    sftp_password = "pass"
                    transport = paramiko.Transport((sftp_host, 22))
                    transport.connect(username=sftp_username, password=sftp_password)
                    sftp = paramiko.SFTPClient.from_transport(transport)
                    
                    # Transfer the file to remote location
                    remote_path = f"/remote/path/unified_output_data_epoch_{epoch_id}_batch_{batch_idx}_file_{file_idx}.csv"
                    sftp.put(file_path, remote_path)
                    
                    # Clean up the temporary file
                    sftp.close()
                    transport.close()
                    print(f"File {file_path} transferred to {remote_path} successfully.")
                except Exception as e:
                    print(f"Failed to transfer file {file_path} to sFTP: {e}")

    # Execute real-time API sync and sFTP batch storage
    unified_output_data.writeStream \
        .foreach(sync_to_api) \
        .outputMode("append") \
        .start()
    # Use foreachBatch to call save_to_sftp for each streaming batch
    unified_output_data.writeStream \
        .foreachBatch(save_to_sftp) \
        .outputMode("append") \
        .start() \
        .awaitTermination()





if __name__ == "__main__":
    create_streaming_pipeline()
    spark.streams.awaitAnyTermination()
