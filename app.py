from clickhouse_client import ClickHouseClient
from datetime import datetime, timezone
from aiohttp import web
import configparser
import logging
import json
import os

config = configparser.ConfigParser()
config.read(os.environ.get("ABUSE_CONFIG_FILE", "config.ini"))
logging.basicConfig(
    format="%(name)s %(asctime)s %(levelname)s %(message)s",
    level=logging.getLevelName(config["logging"]["level"]),
)

logging.info("Serivce started...")


async def post_impressions(request: web.Request):
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
        data = await request.json()
        client = ClickHouseClient(
            host=config["clickhouse"]["host"], username=config["clickhouse"]["username"], port=config["clickhouse"]["port"]
        )
        rows = [list(data.values())]
        client.insert_impressions(rows)
        response_obj = {"status": "success"}
        return web.Response(text=json.dumps(response_obj), status=201)
    except Exception as ex:
        logging.error(ex)
        response_obj = {"status": "failed", "reason": str(ex)}
        return web.Response(text=json.dumps(response_obj), status=500)


async def post_conversions(request: web.Request):
    """ request body example:
        {
            "click_id": "e7028256-0c6e-46f4-bc74-6756274f13dc",
            "revenue": "revenue",
            "var": "var"
        }
    """
    try:
        data = await request.json()
        client = ClickHouseClient(
            host=config["clickhouse"]["host"], username=config["clickhouse"]["username"], port=config["clickhouse"]["port"]
        )
        data["date_time"] = datetime.now(tz=timezone.utc)
        rows = [list(data.values())]
        client.insert_conversions(rows)
        response_obj = {"status": "success"}
        return web.Response(text=json.dumps(response_obj), status=201)
    except Exception as ex:
        logging.error(ex)
        response_obj = {"status": "failed", "reason": str(ex)}
        return web.Response(text=json.dumps(response_obj), status=500)


app = web.Application()

app.router.add_post("/impressions", post_impressions)

app.router.add_post("/conversions", post_conversions)

web.run_app(app)
