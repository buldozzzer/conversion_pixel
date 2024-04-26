import clickhouse_connect
import logging


class ClickHouseClient:
    __table_impressions: str = "impressions"
    __table_conversions: str = "conversions"

    def __init__(
        self,
        host: str,
        username: str,
        password: str = "",
        port: int = 8123,
        db: str = "default",
    ) -> None:
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = password
        self.__db = db

        self.__client = clickhouse_connect.get_client(
            host=self.__host,
            port=self.__port,
            username=self.__username,
            password=self.__password,
        )

        self.init_table()

    def __del__(self):
        self.__client.close()

    def init_table(self):
        self.__client.command(
            """CREATE TABLE IF NOT EXISTS {}.{} 
            (
                click_id String PRIMARY KEY, 
                os String, 
                browser String,
                country String,
                language String,
                compaign_id String,
                source_id String,
                zone_id String
            )
            ENGINE = MergeTree ORDER BY click_id
            """.format(
                self.__db, self.__table_impressions
            )
        )

        self.__client.command(
            """CREATE TABLE IF NOT EXISTS {}.{} 
            (   
                click_id String PRIMARY KEY,
                date_time DateTime('Europe/London'),
                revenue String,
                var String
            )
            ENGINE = MergeTree ORDER BY click_id
            """.format(
                self.__db, self.__table_conversions
            )
        )

    def drop_table(self):
        self.__client.command(
            "DROP TABLE IF EXISTS {}.{}".format(self.__db, self.__table_impressions)
        )
        self.__client.command(
            "DROP TABLE IF EXISTS {}.{}".format(self.__db, self.__table_conversions)
        )

    def insert_impressions(self, rows: list):
        written_rows = self.__client.insert(
            table=self.__table_impressions,
            data=rows,
            column_names=[
                "click_id",
                "os",
                "browser",
                "country",
                "language",
                "compaign_id",
                "source_id",
                "zone_id",
            ],
        ).written_rows
        logging.debug(f"Inserted {len(rows)} row(s)")

    def insert_conversions(self, rows: list):
        written_rows = self.__client.insert(
            table=self.__table_conversions,
            data=rows,
            column_names=[
                "click_id",
                "revenue",
                "var",
                "date_time",
            ],
        ).written_rows
        logging.debug(f"Inserted {len(rows)} row(s)")
