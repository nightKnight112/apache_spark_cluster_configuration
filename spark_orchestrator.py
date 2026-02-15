from pyspark.sql import SparkSession
import os
import yaml


with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)



def initalize_spark_session_and_use_cluster():
    # -------------------------------------------------------
    # 1. Create Spark Session (Standalone Cluster)
    # -------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("DB-ETL-Clustering-Service")
        .master(f"{config['spark_master']}")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.jars", "/usr/src/app/jars/postgresql-42.7.3.jar")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # -------------------------------------------------------
    # 2. READ FROM SOURCE DATABASE → DataFrame
    # -------------------------------------------------------

    jdbc_url = "jdbc:postgresql://10.88.0.1:8100/postgres"
    source_table = "public.customers"

    connection_properties = {
        "user": "postgres",
        "password": "passwordadminsource",
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

    parquet_path = f"{config['spark_volume_mount_data_path']}"

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

    target_jdbc_url = "jdbc:postgresql://10.88.0.1:8101/postgres"
    target_table = "public.customers"

    target_connection_properties = {
        "user": "postgres",
        "password": "passwordadmintarget",
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