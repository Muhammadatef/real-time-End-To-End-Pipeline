from pyflink.datastream import StreamExecutionEnvironment, SourceFunction
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Schema
from pyflink.table.udf import udf
from pyflink.table.expressions import col, lit
from pyflink.common import DataTypes
from pyflink.table.window import Tumble
import hashlib
from influxdb_client import InfluxDBClient, Point, WritePrecision
import requests
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# InfluxDB configuration
INFLUXDB_URL = "http://influxdb:8086"
INFLUXDB_TOKEN = "32xn55CeJleTx74AflUjqhMYAN_Ko6VsKlqayFz-uE-IVqiMFYN3VsjcrUODm9MDclM_s9v4bXkaENsYaVt5Yg=="
INFLUXDB_ORG = "NA"
INFLUXDB_BUCKET = "sales_data"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def write_batch_to_influxdb(measurement, data_points):
    try:
        with InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG) as client:
            write_api = client.write_api(write_options=WritePrecision.NS)
            points = [Point(measurement).field(key, value) if isinstance(value, (int, float))
                      else Point(measurement).tag(key, value) for key, value in point.items()] 
            write_api.write(INFLUXDB_BUCKET, INFLUXDB_ORG, points)
    except Exception as e:
        logger.error(f"Failed to write {measurement} with data {data_points} to InfluxDB: {e}")

# Custom Source Function to poll API data
class SalesTransactionAPISource(SourceFunction):
    def run(self, ctx):
        url = "http://api-server:5002/api/customer_data"
        while True:
            try:
                # Poll the Flask API for real-time sales transaction data
                response = requests.get(url)
                if response.status_code == 200:
                    sales_data = response.json()  # API should return JSON format data
                    for transaction in sales_data:
                        # Collect each transaction data
                        ctx.collect(transaction)
                else:
                    print(f"API call failed with status {response.status_code}")
                
                # Sleep interval to control polling rate
                time.sleep(5)  # Poll every 5 seconds
            except Exception as e:
                print(f"Error fetching data from API: {e}")
    
    def cancel(self):
        pass



@udf(result_type=DataTypes.STRING())
def sha256_hash(input_str: str) -> str:
    return hashlib.sha256(input_str.encode()).hexdigest() if input_str else None

@udf(result_type=Types.STRING())
def get_age_band(age: int) -> str:
    if age is None:
        return None
    if age < 18:
        return '<18'
    elif age <= 30:
        return '19-30'
    elif age <= 50:
        return '31-50'
    else:
        return '51+'
  
        
def create_pipeline():

    # Set up the StreamExecutionEnvironment and StreamTableEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=env_settings)

    # Register UDFs
    t_env.create_temporary_function("hash_mobile", hash_mobile)
    t_env.create_temporary_function("get_age_band", get_age_band)

            # Unified GPS Data Source
    def create_kafka_source_ddl():
        return """
        CREATE TABLE gps_data (
            SourceID STRING,
            CustomerID STRING,
            CarID STRING,
            OfficeID STRING,
            AgentID STRING,
            TrxnTimestamp TIMESTAMP(3),
            CarDrivingStatus STRING,
            CurrentLongitude DOUBLE,
            CurrentLatitude DOUBLE,
            CurrentArea STRING,
            KM DOUBLE,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'gps_data',  -- Unified topic for all GPS sources
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'gps-consumer-group',
            'scan.startup.mode' = 'latest',
            'format' = 'json'
        )
        """

    #Kafka_source_office_data
    def create_office_data_source():
        return """
        CREATE TABLE office_data (
            OfficeID STRING,
            MobileNo STRING,
            Area STRING,
            OfficeNo STRING,
            WorkingHours STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'office_data',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'office_consumer_group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
        """

    #Kafka_source__agent_data
    def create_agent_data_source():
        return """
        CREATE TABLE agent_data (
            AgentID STRING,
            MobileNo STRING,
            Name STRING,
            Gender STRING,
            Age INT,
            Nationality STRING,
            OfficeID STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'agent_data',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'agent_consumer_group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
        """
    def create_kafka_sales_source_ddl():
        return """
        CREATE TABLE sales_data (
            AgentID STRING,
            OfficeID STRING,
            CarID STRING,
            CustomerID STRING,
            Amount DOUBLE,
            TrxnTimestamp TIMESTAMP(3),
            WATERMARK FOR TrxnTimestamp AS TrxnTimestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales_data',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'sales-consumer-group',
            'scan.startup.mode' = 'latest',
            'format' = 'json'
        )
        """
    # Register tables in PyFlink environment
    t_env.execute_sql(create_office_data_source())
    t_env.execute_sql(create_agent_data_source())
    t_env.execute_sql(create_kafka_sales_source_ddl())




    # Execute the DDL
    sales_data_table = t_env.from_path("sales_data")


    enriched_data = t_env.sql_query(""" 
        SELECT 
            gps.SourceID,
            gps.CustomerID,
            gps.CarID,
            gps.OfficeID,
            gps.AgentID,
            gps.TrxnTimestamp,
            gps.CarDrivingStatus,
            gps.CurrentLongitude,
            gps.CurrentLatitude,
            gps.CurrentArea,
            gps.KM,
            sales.Amount AS SalesAmount,
            office.MobileNo AS OfficeMobile,
            office.Area AS OfficeArea,
            office.OfficeNo,
            office.WorkingHours,
            agent.Name AS AgentName,
            agent.Gender AS AgentGender,
            agent.Age AS AgentAge,
            agent.Nationality AS AgentNationality,
            customer.MobileNo AS CustomerMobileNo,
            customer.Name AS CustomerName,
            customer.Gender AS CustomerGender,
            customer.Age AS CustomerAge,
            customer.Nationality AS CustomerNationality
        FROM gps_data AS gps
        LEFT JOIN sales_data AS sales ON gps.CustomerID = sales.CustomerID
            AND gps.CarID = sales.CarID 
            AND gps.OfficeID = sales.OfficeID 
            AND gps.AgentID = sales.AgentID
        LEFT JOIN customer_data AS customer ON gps.CustomerID = customer.CustomerID
        LEFT JOIN office_data AS office ON gps.OfficeID = office.OfficeID
        LEFT JOIN agent_data AS agent ON gps.AgentID = agent.AgentID
        WHERE gps.CurrentArea = 'Dubai'
    """)



    #Transformations, aggregations, validation and data quality checks

    # Transformations, aggregations, validation, and data quality checks

    cleaned_sales_data = sales_data_table \
        .filter(col("AgentID").is_not_null() & col("OfficeID").is_not_null() & (col("Amount") > 0)) \
        .filter(col("CurrentLongitude").between(55.1, 55.4) & col("CurrentLatitude").between(25.0, 25.3))  # Dubai Lat & Long

    # Monthly sales count
    sales_agg_monthly = cleaned_sales_data \
        .window(Tumble.over(lit(1).month).on(col("TrxnTimestamp")).alias("month_window")) \
        .group_by(col("month_window")) \
        .select(col("month_window").start.alias("month"), col("AgentID").count.alias("sales_count"))

    # Monthly sales volume
    monthly_sales_volume = cleaned_sales_data \
        .window(Tumble.over(lit(1).month).on(col("TrxnTimestamp")).alias("month_window")) \
        .group_by(col("month_window")) \
        .select(col("month_window").start.alias("month"), col("Amount").sum.alias("sales_volume"))


    # Car Driving Status count per area
    car_status_count = cleaned_sales_data \
        .group_by("CurrentArea, CarDrivingStatus") \
        .select("CurrentArea, CarDrivingStatus, COUNT(1) AS status_count")

    # Step 6: Anonymize sensitive data (e.g., mobile numbers)
    anonymized_data = enriched_data \
        .select("*", sha2(col("CustomerPhoneNumber"), 256).alias("AnonymizedPhoneNumber"))



    # Define the Kafka Sink DDL for the processed data
    def create_kafka_sink_ddl(topic_name):
        return f"""
        CREATE TABLE processed_cleaned_sales_data (
            AgentID STRING,
            OfficeID STRING,
            CarID STRING,
            CustomerID STRING,
            Amount DOUBLE,
            TrxnTimestamp TIMESTAMP(3),
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic_name}',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json',
            'sink.partitioner' = 'roundrobin'  -- or other partitioning strategy
        )
        """

    # Execute the DDL to create the sink table
    t_env.execute_sql(create_kafka_sink_ddl('processed_cleaned_sales_data'))

    # Assuming cleaned_sales_data is your processed table
    t_env.execute_sql("""
        INSERT INTO processed_cleaned_sales_data
        SELECT *
        FROM cleaned_sales_data
    """)



    # Define Kafka sink for enriched sales data
    def create_kafka_sink_for_enriched_data():
        return """
        CREATE TABLE enriched_data (
            CustomerID STRING,
            CarID STRING,
            AgentID STRING,
            OfficeID STRING,
            Amount DOUBLE,
            TrxnTimestamp TIMESTAMP(3),
            OfficeMobile STRING,
            OfficeArea STRING,
            OfficeNo STRING,
            WorkingHours STRING,
            AgentName STRING,
            AgentGender STRING,
            AgentAge INT,
            AgentNationality STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'enriched_data',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json',
            'sink.partitioner' = 'roundrobin'
        )
        """

    t_env.execute_sql(create_kafka_sink_for_enriched_data())



    t_env.execute_sql(create_kafka_sink_for_enriched_data())
    enriched_data.execute_insert("enriched_data")


    # Define schema for the sales transactions API data
    sales_schema = t_env.from_data_stream(
        env.add_source(SalesTransactionAPISource()).name("sales_transactions"),
        schema=Schema.new_builder()
            .column("AgentID", DataTypes.STRING())
            .column("OfficeID", DataTypes.STRING())
            .column("CarID", DataTypes.STRING())
            .column("CustomerID", DataTypes.STRING())
            .column("Amount", DataTypes.DOUBLE())
            .column("TrxnTimestamp", DataTypes.TIMESTAMP(3))
            .build()
    )

    # Register sales transaction data as a table
    t_env.create_temporary_view("sales_transactions", sales_schema)


    # Enriched data view
    t_env.execute_sql("""    
        CREATE VIEW enriched_data AS
        SELECT 
            g.*,
            c.MobileNo,
            hash_mobile(c.MobileNo) as HashedMobileNo,
            c.Name as CustomerName,
            c.Gender as CustomerGender,
            c.Age as CustomerAge,
            get_age_band(c.Age) as AgeBand,
            c.Nationality as CustomerNationality,
            c.PassportNo,
            c.IDNo,
            c.HomeAddress,
            c.LeaseStartDate,
            c.LeasePeriod,
            cd.CarMake,
            cd.CarModel,
            cd.PlateNo,
            cd.RegistrationDate,
            cd.RegistrationExpiryDate,
            o.MobileNo as OfficeMobileNo,
            o.Area as OfficeArea,
            o.OfficeNo,
            o.WorkingHours,
            a.MobileNo as AgentMobileNo,
            a.Name as AgentName,
            a.Gender as AgentGender,
            a.Age as AgentAge,
            a.Nationality as AgentNationality
        FROM unified_gps g
        LEFT JOIN customer_data c ON g.CustomerID = c.CustomerID
        LEFT JOIN car_data cd ON g.CarID = cd.CarID
        LEFT JOIN office_data o ON g.OfficeID = o.OfficeID
        LEFT JOIN agent_data a ON g.AgentID = a.AgentID
    """)

    # Filtered view for data specific to 'Dubai'
    t_env.execute_sql("""    
        CREATE VIEW dubai_data AS
        SELECT * FROM enriched_data
        WHERE CurrentArea = 'Dubai'
    """)

    # Create aggregation views
    t_env.execute_sql("""    
        CREATE VIEW sales_aggregates AS
        SELECT 
            EXTRACT(YEAR FROM TrxnTimestamp) as Year,
            EXTRACT(MONTH FROM TrxnTimestamp) as Month,
            COUNT(*) as SalesCount,
            SUM(Amount) as SalesVolume
        FROM sales_transactions
        GROUP BY 
            EXTRACT(YEAR FROM TrxnTimestamp),
            EXTRACT(MONTH FROM TrxnTimestamp)
    """)

    t_env.execute_sql("""    
        CREATE VIEW driving_status_aggregates AS
        SELECT 
            CurrentArea,
            CarDrivingStatus,
            COUNT(*) as StatusCount
        FROM unified_gps
        GROUP BY CurrentArea, CarDrivingStatus
    """)

    # Create SFTP sink with batching
    t_env.execute_sql("""    
        CREATE TABLE sftp_output (
            BatchID STRING,
            TrxnTimestamp TIMESTAMP(3),
            CarDrivingStatus STRING,
            CurrentLongitude DOUBLE,
            CurrentLatitude DOUBLE,
            CurrentArea STRING,
            KM DOUBLE,
            HashedMobileNo STRING,
            CustomerName STRING,
            CustomerGender STRING,
            AgeBand STRING,
            CustomerNationality STRING,
            CarMake STRING,
            CarModel STRING,
            PlateNo STRING,
            OfficeArea STRING,
            AgentName STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/output',
            'format' = 'csv',
            'csv.field-delimiter' = ',',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '1 min'
        )
    """)

    # Insert into SFTP output with batching logic
    t_env.execute_sql("""    
        INSERT INTO sftp_output
        SELECT 
            CAST(FLOOR(RAND() * 1000000) AS STRING) as BatchID,
            TrxnTimestamp,
            CarDrivingStatus,
            CurrentLongitude,
            CurrentLatitude,
            CurrentArea,
            KM,
            HashedMobileNo,
            CustomerName,
            CustomerGender,
            AgeBand,
            CustomerNationality,
            CarMake,
            CarModel,
            PlateNo,
            OfficeArea,
            AgentName
        FROM dubai_data
        WHERE ROW_NUMBER() OVER (PARTITION BY TrxnTimestamp ORDER BY TrxnTimestamp) <= 2000
    """)


    def stream_enriched_data():
        # Stream enriched sales data
        for row in enriched_data.execute().collect():
            data = {
                "CustomerID": row["CustomerID"],
                "CarID": row["CarID"],
                "AgentID": row["AgentID"],
                "OfficeID": row["OfficeID"],
                "Amount": row["Amount"],
                "TrxnTimestamp": row["TrxnTimestamp"].isoformat(),
                "OfficeMobile": row["OfficeMobile"],
                "OfficeArea": row["OfficeArea"],
                "AgentName": row["AgentName"]
            }
            write_batch_to_influxdb("enriched_data", data)

    def stream_cleaned_sales_data():
        # Stream cleaned sales data
        cleaned_sales_data = sales_data_table \
            .filter(col("AgentID").is_not_null() & col("OfficeID").is_not_null() & (col("Amount") > 0)) \
            .filter(col("CurrentLongitude").between(55.1, 55.4) & col("CurrentLatitude").between(25.0, 25.3))

        for row in cleaned_sales_data.execute().collect():
            data = {
                "AgentID": row["AgentID"],
                "OfficeID": row["OfficeID"],
                "Amount": row["Amount"],
                "CurrentLongitude": row["CurrentLongitude"],
                "CurrentLatitude": row["CurrentLatitude"]
            }
            write_batch_to_influxdb("cleaned_sales_data", data)

    def stream_monthly_sales_volume():
        # Stream monthly sales volume
        monthly_sales_volume = cleaned_sales_data \
            .window(Tumble.over(lit(1).month).on("TrxnTimestamp").alias("month_window")) \
            .group_by("month_window") \
            .select("month_window.start AS month, SUM(Amount) AS sales_volume")

        for row in monthly_sales_volume.execute().collect():
            data = {
                "month": row["month"].isoformat(),
                "sales_volume": row["sales_volume"]
            }
            write_batch_to_influxdb("monthly_sales_volume", data)

    def stream_sales_agg_monthly():
        # Stream monthly sales aggregate counts
        sales_agg_monthly = cleaned_sales_data \
            .window(Tumble.over(lit(1).month).on("TrxnTimestamp").alias("month_window")) \
            .group_by("month_window") \
            .select("month_window.start AS month, COUNT(AgentID) AS sales_count")

        for row in sales_agg_monthly.execute().collect():
            data = {
                "month": row["month"].isoformat(),
                "sales_count": row["sales_count"]
            }
            write_batch_to_influxdb("sales_agg_monthly", data)

    def stream_car_status_count():
        # Stream car driving status count per area
        car_status_count = cleaned_sales_data \
            .group_by("CurrentArea, CarDrivingStatus") \
            .select("CurrentArea, CarDrivingStatus, COUNT(1) AS status_count")

        for row in car_status_count.execute().collect():
            data = {
                "CurrentArea": row["CurrentArea"],
                "CarDrivingStatus": row["CarDrivingStatus"],
                "status_count": row["status_count"]
            }
            write_batch_to_influxdb("car_status_count", data)

    def stream_data_to_influx_async():
        # Run each data streaming function concurrently
        with ThreadPoolExecutor() as executor:
            executor.submit(stream_enriched_data)
            executor.submit(stream_cleaned_sales_data)
            executor.submit(stream_monthly_sales_volume)
            executor.submit(stream_sales_agg_monthly)
            executor.submit(stream_car_status_count)

    if __name__ == '__main__':
        create_pipeline()
        stream_data_to_influx_async()  # Start concurrent InfluxDB data streaming