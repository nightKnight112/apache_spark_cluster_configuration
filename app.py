from flask import Flask, jsonify, request
from flask_cors import CORS
from spark_orchestrator import initialize_spark_session


app = Flask(__name__)
CORS(app)

@app.route('/initialize-spark', methods=['POST'])
def initialize_spark():
    result, status = initialize_spark_session()
    return jsonify(result), status


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)