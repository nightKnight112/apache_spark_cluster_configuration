from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os
import yaml


with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)



def initalize_spark_session_and_use_cluster():
    spark = (
        SparkSession.builder
        .appName("DB-ETL-Clustering-Service")
        .master(f"{config["spark_master"]}")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.jars", "/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------------------------
    # 1. Create Spark Session (Standalone Cluster)
    # -------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("DB-ETL-Clustering-Service")
        .master(f"{config["spark_master"]}")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.jars", "/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------------------------
    # 2. READ FROM SOURCE DATABASE → DataFrame
    # -------------------------------------------------------

    jdbc_url = "jdbc:postgresql://source-db:5432/source_database"
    source_table = "public.source_table"

    connection_properties = {
        "user": "source_user",
        "password": "source_password",
        "driver": "org.postgresql.Driver"
    }

    source_df = (
        spark.read
        .jdbc(
            url=jdbc_url,
            table=source_table,
            properties=connection_properties
        )
    )

    print("Source Schema:")
    source_df.printSchema()

    # -------------------------------------------------------
    # 3. WRITE TO PARQUET (Cluster Shared Volume)
    # -------------------------------------------------------

    parquet_path = f"{config["spark_volume_mount_data_path"]}"

    (
        source_df.write
        .mode("overwrite")
        .parquet(parquet_path)
    )

    print("Data written to parquet stage.")

    # -------------------------------------------------------
    # 4. READ PARQUET BACK → DataFrame
    # -------------------------------------------------------

    reloaded_df = spark.read.parquet(parquet_path)

    print("Reloaded Schema:")
    reloaded_df.printSchema()

    # -------------------------------------------------------
    # 6. WRITE TO TARGET DATABASE
    # -------------------------------------------------------

    target_jdbc_url = "jdbc:postgresql://target-db:5432/target_database"
    target_table = "public.target_table"

    target_connection_properties = {
        "user": "target_user",
        "password": "target_password",
        "driver": "org.postgresql.Driver"
    }

    (
        reloaded_df.write
        .mode("append")  # use overwrite if needed
        .jdbc(
            url=target_jdbc_url,
            table=target_table,
            properties=target_connection_properties
        )
    )

    print("Data written to target database.")

    # -------------------------------------------------------
    # 7. Stop Session
    # -------------------------------------------------------

    spark.stop()
    return {"status": "success", "message": "Spark session initialized and data processed."}, 200