from clickhouse_client import ClickHouseClient
from datetime import datetime, timezone
import configparser
import logging
import os
from flask import Flask, request, jsonify, make_response, Response

app = Flask(__name__)

config = configparser.ConfigParser()
config.read(os.environ.get("CONFIG_FILE", "config.ini"))
logging.basicConfig(
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    level=logging.getLevelName(config["logging"]["level"]),
)


def _build_cors_preflight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "*")
    response.headers.add("Access-Control-Allow-Methods", "*")
    return response

def _corsify_actual_response(response: Response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.route("/impressions", methods=["POST", "OPTIONS"])
def post_impressions():
    """request body example:
    {
        "click_id": "e7028256-0c6e-46f4-bc74-6756274f13dc",
        "os": "Windows",
        "browser": "Chrome 123",
        "country": "Russian Federation",
        "language": "ru",
        "compaign_id": "compaign_id",
        "source_id": "source_id",
        "zone_id": "zone_id"
    }
    """
    try:
        if request.method == "OPTIONS":
            return _build_cors_preflight_response()
        
        data = request.json
        client = ClickHouseClient(
            host=config["clickhouse"]["host"],
            username=config["clickhouse"]["username"],
            port=config["clickhouse"]["port"],
        )
        rows = [list(data.values())]
        client.insert_impressions(rows)

        return _corsify_actual_response(jsonify({"status": "success"})), 201
    except Exception as ex:
        logging.error(ex)
        return _corsify_actual_response(jsonify({"status": "failed", "reason": str(ex)})), 500


@app.route("/conversions", methods=["POST", "OPTIONS"])
def post_conversions():
    """request body example:
    {
        "click_id": "e7028256-0c6e-46f4-bc74-6756274f13dc",
        "revenue": "revenue",
        "var": "var"
    }
    """
    try:
        if request.method == "OPTIONS":
            return _build_cors_preflight_response()
        
        data = request.json
        client = ClickHouseClient(
            host=config["clickhouse"]["host"],
            username=config["clickhouse"]["username"],
            port=config["clickhouse"]["port"],
        )
        data["date_time"] = datetime.now(tz=timezone.utc)
        rows = [list(data.values())]
        client.insert_conversions(rows)

        return _corsify_actual_response(jsonify({"status": "success"})), 201
    except Exception as ex:
        logging.error(ex)
        return _corsify_actual_response(jsonify({"status": "failed", "reason": str(ex)})), 500


if __name__ == "__main__":
    logging.info("Serivce started...")
    app.run(debug=False, host="0.0.0.0", port=8080)
