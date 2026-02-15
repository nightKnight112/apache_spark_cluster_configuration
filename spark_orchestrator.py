from pyspark.sql.functions import col
from pyspark.sql.types import NullType, DecimalType


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

        print("SparkContext stopped?:", spark.sparkContext._jsc.sc().isStopped())

        row_count = source_df.count()
        print("Number of rows:", row_count)

    except Exception as e:
        print("Error reading from source database:", str(e))
        return {"status": "error", "message": str(e)}, 500


    # -------------------------------------------------------
    # TEST 1 — Check for NullType columns
    # -------------------------------------------------------

    nulltype_cols = [f.name for f in source_df.schema.fields if isinstance(f.dataType, NullType)]

    if nulltype_cols:
        print("❌ NullType columns found:", nulltype_cols)
        return {"status": "error", "message": f"NullType columns found: {nulltype_cols}"}, 500
    else:
        print("✅ No NullType columns found")


    # -------------------------------------------------------
    # TEST 2 — Check for High Precision Decimals
    # -------------------------------------------------------

    decimal_cols = []
    for f in source_df.schema.fields:
        if isinstance(f.dataType, DecimalType):
            decimal_cols.append((f.name, f.dataType.precision, f.dataType.scale))

    if decimal_cols:
        print("⚠ Decimal columns found:", decimal_cols)
        print("Casting decimals to double for safety...")
        for col_name, _, _ in decimal_cols:
            source_df = source_df.withColumn(col_name, col(col_name).cast("double"))
    else:
        print("✅ No Decimal columns found")


    # -------------------------------------------------------
    # TEST 3 — Normalize Column Names
    # -------------------------------------------------------

    source_df = source_df.toDF(*[c.lower() for c in source_df.columns])
    print("✅ Column names normalized to lowercase")


    # -------------------------------------------------------
    # TEST 4 — Minimal Parquet Write Test
    # -------------------------------------------------------

    try:
        source_df.limit(1).write.mode("overwrite").parquet("/opt/spark/data/test_minimal")
        print("✅ Minimal parquet write succeeded")
    except Exception as e:
        print("❌ Minimal parquet write failed:", str(e))
        return {"status": "error", "message": f"Minimal parquet test failed: {str(e)}"}, 500


    # -------------------------------------------------------
    # TEST 5 — Explain Plan (Detect GPU/RAPIDS)
    # -------------------------------------------------------

    print("Execution Plan:")
    source_df.explain(True)


    # -------------------------------------------------------
    # WRITE TO PARQUET (MAIN)
    # -------------------------------------------------------

    parquet_path = "/opt/spark/data"

    try:
        source_df.write.mode("overwrite").parquet(parquet_path)
        print("✅ Data written to parquet stage.")
    except Exception as e:
        print("❌ Error writing to parquet:", str(e))
        return {"status": "error", "message": str(e)}, 500


    # -------------------------------------------------------
    # READ PARQUET BACK
    # -------------------------------------------------------

    try:
        reloaded_df = spark.read.parquet(parquet_path)
        print("Reloaded Schema:")
        reloaded_df.printSchema()
    except Exception as e:
        print("❌ Error reading from parquet:", str(e))
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
        print("✅ Data written to target database.")
    except Exception as e:
        print("❌ Error writing to target database:", str(e))
        return {"status": "error", "message": str(e)}, 500

    return {"status": "success", "rows_processed": row_count}, 200
