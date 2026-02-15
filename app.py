from flask import Flask, jsonify, request
from flask_cors import CORS
from spark_orchestrator import initalize_spark_session_and_use_cluster
import yaml
from pyspark.sql import SparkSession


app = Flask(__name__)
CORS(app)

spark = None


def create_spark():
    global spark

    if spark is None:
        with open("config.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)

        spark = (
            SparkSession.builder
            .appName("DB-ETL-Clustering-Service")
            .master(config['spark_master'])
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "2")
            .config("spark.driver.memory", "1g")
            .config("spark.jars", "/usr/src/app/jars/postgresql-42.7.3.jar")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")
        print("Spark initialized inside Flask app")

    return spark

@app.route('/initialize-spark', methods=['POST'])
def initialize_spark():
    spark = create_spark()
    result, status = initalize_spark_session_and_use_cluster(spark=spark)
    return jsonify(result), status


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=False)