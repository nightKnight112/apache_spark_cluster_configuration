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
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    print("Spark Session initialized with cluster master:", config['spark_master'])
    print("Spark Version:", spark.version)

    # -------------------------------------------------------
    # 2. READ FROM SOURCE DATABASE → DataFrame
    # -------------------------------------------------------

    jdbc_url = "jdbc:postgresql://postgres-source:5432/postgres"
    source_table = "public.customers"

    connection_properties = {
        "user": "postgres",
        "password": "passwordadminsource",
        "driver": "org.postgresql.Driver"
    }

    try:
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
        print("Number of rows to write:", source_df.count())
    except Exception as e:
        print("Error reading from source database:", str(e))
        return {"status": "error", "message": "Failed to read from source database."}, 500

    # -------------------------------------------------------
    # 3. WRITE TO PARQUET (Cluster Shared Volume)
    # -------------------------------------------------------
    parquet_path = f"file://{config['spark_volume_mount_data_path']}"

    try:

        (
            source_df.write
            .mode("overwrite")
            .parquet(parquet_path)
        )

        print("Data written to parquet stage.")
    except Exception as e:
        print("Error writing to parquet:", str(e))
        return {"status": "error", "message": "Failed to write to parquet."}, 500

    # -------------------------------------------------------
    # 4. READ PARQUET BACK → DataFrame
    # -------------------------------------------------------
    try:
        reloaded_df = spark.read.parquet(parquet_path)

        print("Reloaded Schema:")
        reloaded_df.printSchema()
    except Exception as e:
        print("Error reading from parquet:", str(e))
        return {"status": "error", "message": "Failed to read from parquet."}, 500

    # -------------------------------------------------------
    # 6. WRITE TO TARGET DATABASE
    # -------------------------------------------------------

    target_jdbc_url = "jdbc:postgresql://postgres-target:5432/postgres"
    target_table = "public.customers"

    target_connection_properties = {
        "user": "postgres",
        "password": "passwordadmintarget",
        "driver": "org.postgresql.Driver"
    }
    try:
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
    except Exception as e:
        print("Error writing to target database:", str(e))
        return {"status": "error", "message": "Failed to write to target database."}, 500

    # -------------------------------------------------------
    # 7. Stop Session
    # -------------------------------------------------------

    spark.stop()
    return {"status": "success", "message": "Spark session initialized and data processed."}, 200