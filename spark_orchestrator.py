from pyspark.sql import SparkSession
import yaml

# -------------------------------------------------------
# Load Config
# -------------------------------------------------------

with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)


# -------------------------------------------------------
# API Callable Function
# -------------------------------------------------------

def initalize_spark_session_and_use_cluster(spark=None):

    print("Using existing Spark Session")

    # -------------------------------------------------------
    # READ FROM SOURCE DATABASE
    # -------------------------------------------------------

    jdbc_url = "jdbc:postgresql://100.53.177.194:8100/postgres"
    source_table = "public.customers"

    connection_properties = {
        "user": "postgres",
        "password": "source",
        "driver": "org.postgresql.Driver"
    }

    try:
        source_df = spark.read.jdbc(
            url=jdbc_url,
            table=source_table,
            properties=connection_properties
        )

        print("Source Schema:")
        source_df.printSchema()

        row_count = source_df.count()
        print("Number of rows:", row_count)

    except Exception as e:
        print("Error reading from source database:", str(e))
        return {"status": "error", "message": str(e)}, 500

    # -------------------------------------------------------
    # WRITE TO PARQUET
    # -------------------------------------------------------

    parquet_path = config['spark_volume_mount_data_path']  # ❌ removed file://

    try:
        source_df.write.mode("overwrite").parquet(parquet_path)
        print("Data written to parquet stage.")

    except Exception as e:
        print("Error writing to parquet:", str(e))
        return {"status": "error", "message": str(e)}, 500

    # -------------------------------------------------------
    # READ PARQUET BACK
    # -------------------------------------------------------

    try:
        reloaded_df = spark.read.parquet(parquet_path)
        print("Reloaded Schema:")
        reloaded_df.printSchema()

    except Exception as e:
        print("Error reading from parquet:", str(e))
        return {"status": "error", "message": str(e)}, 500

    # -------------------------------------------------------
    # WRITE TO TARGET DATABASE
    # -------------------------------------------------------

    target_jdbc_url = "jdbc:postgresql://100.53.177.194:8101/postgres"
    target_table = "public.customers"

    target_connection_properties = {
        "user": "postgres",
        "password": "target",
        "driver": "org.postgresql.Driver"
    }

    try:
        reloaded_df.write.mode("append").jdbc(
            url=target_jdbc_url,
            table=target_table,
            properties=target_connection_properties
        )

        print("Data written to target database.")

    except Exception as e:
        print("Error writing to target database:", str(e))
        return {"status": "error", "message": str(e)}, 500

    # 🚨 DO NOT STOP SPARK HERE

    return {"status": "success", "rows_processed": row_count}, 200