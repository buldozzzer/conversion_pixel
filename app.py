from clickhouse_client import ClickHouseClient
from datetime import datetime, timezone
import configparser
import logging
import os
from flask import Flask, request, jsonify

app = Flask(__name__)

config = configparser.ConfigParser()
config.read(os.environ.get("CONFIG_FILE", "config.ini"))
logging.basicConfig(
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    level=logging.getLevelName(config["logging"]["level"]),
)

@app.route("/impressions", methods=['POST'])
def post_impressions():
    """ request body example:
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
        data = request.data
        client = ClickHouseClient(
            host=config["clickhouse"]["host"], username=config["clickhouse"]["username"], port=config["clickhouse"]["port"]
        )
        rows = [list(data.values())]
        client.insert_impressions(rows)
        
        return jsonify({"status": "success"}), 201
    except Exception as ex:
        logging.error(ex)
        return jsonify({"status": "failed", "reason": str(ex)}), 500

@app.route('/conversions', methods=['POST'])
def post_conversions():
    """ request body example:
        {
            "click_id": "e7028256-0c6e-46f4-bc74-6756274f13dc",
            "revenue": "revenue",
            "var": "var"
        }
    """
    try:
        data = request.data
        client = ClickHouseClient(
            host=config["clickhouse"]["host"], username=config["clickhouse"]["username"], port=config["clickhouse"]["port"]
        )
        data["date_time"] = datetime.now(tz=timezone.utc)
        rows = [list(data.values())]
        client.insert_conversions(rows)
        
        return jsonify({"status": "success"}), 201
    except Exception as ex:
        logging.error(ex)
        return jsonify({"status": "failed", "reason": str(ex)}), 500

if __name__ == '__main__':
    logging.info("Serivce started...")
    app.run(debug = True, port = 8080) 
