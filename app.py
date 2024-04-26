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
    try:
        data = await request.json()
        print(request.headers.get("User-Agent"))
        client = ClickHouseClient(
            host=config["clickhouse"]["host"], username=config["clickhouse"]["username"]
        )
        # data = [(data["click_id"],
        #         data["os"],
        #         data["browser"],
        #         data["country"],
        #         data["language"],
        #         data["compaign_id"],
        #         data["source_id"],
        #         data["zone_id"])]
        data = [data]
        response_obj = {"status": "success"}
        return web.Response(text=json.dumps(response_obj), status=200)
    except Exception as ex:
        logging.error(ex)
        response_obj = {"status": "failed", "reason": str(ex)}
        return web.Response(text=json.dumps(response_obj), status=500)


async def post_conversions(request: web.Request):
    try:
        data = request.json()
        client = ClickHouseClient(
            host=config["clickhouse"]["host"], username=config["clickhouse"]["username"]
        )
        # data = [(data["click_id"],
        #         data["date_time"],
        #         data["revenue"],
        #         data["var"])]
        data = [data]
        for d in data:
            d["date_time"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        client.insert_conversions(data)
        response_obj = {"status": "success"}
        return web.Response(text=json.dumps(response_obj), status=200)
    except Exception as ex:
        logging.error(ex)
        response_obj = {"status": "failed", "reason": str(ex)}
        return web.Response(text=json.dumps(response_obj), status=500)


app = web.Application()

app.router.add_post("/impressions", post_impressions)

app.router.add_post("/conversions", post_impressions)

web.run_app(app)
