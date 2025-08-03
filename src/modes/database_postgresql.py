#!/usr/bin/env python3
"""
Mode S のメッセージを保管し，条件にマッチしたものを出力します．

Usage:
  database_postgres.py [-c CONFIG]

Options:
  -c CONFIG     : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
"""

import datetime
import logging
import queue
import threading
import time

import psycopg2
import psycopg2.extras

should_terminate = threading.Event()


def open(host, port, database, user, password):  # noqa: A001
    connection_params = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }

    try:
        conn = psycopg2.connect(**connection_params)
    except psycopg2.OperationalError as e:
        if "does not exist" in str(e):
            # データベースが存在しない場合、postgresデータベースに接続して作成
            admin_params = connection_params.copy()
            admin_params["database"] = "postgres"

            admin_conn = psycopg2.connect(**admin_params)
            admin_conn.autocommit = True

            with admin_conn.cursor() as cur:
                # データベース名をエスケープしてSQLインジェクションを防ぐ
                cur.execute(f"CREATE DATABASE {psycopg2.extensions.quote_ident(database, admin_conn)}")

            admin_conn.close()

            # 新しく作成したデータベースに接続
            conn = psycopg2.connect(**connection_params)
        else:
            raise

    conn.autocommit = True

    with conn.cursor() as cur:
        # テーブルを再作成する場合は削除
        # cur.execute("DROP TABLE IF EXISTS meteorological_data CASCADE;")

        cur.execute(
            "CREATE TABLE IF NOT EXISTS meteorological_data ("
            "id SERIAL PRIMARY KEY, "
            "time TIMESTAMP NOT NULL, "
            "callsign TEXT NOT NULL, "
            "altitude REAL, "
            "latitude REAL, "
            "longitude REAL, "
            "temperature REAL, "
            "wind_x REAL, "
            "wind_y REAL, "
            "wind_angle REAL, "
            "wind_speed REAL"
            ");"
        )

        # 個別インデックス（単一カラムでの範囲検索用）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time ON meteorological_data (time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_altitude ON meteorological_data (altitude);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_latitude ON meteorological_data (latitude);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_longitude ON meteorological_data (longitude);")

        # 複合インデックス（よく使われる組み合わせ）
        # 時刻と位置情報の組み合わせ
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_time_lat_lon ON meteorological_data (time, latitude, longitude);"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time_alt ON meteorological_data (time, altitude);")

        # 位置情報の組み合わせ（地理的範囲検索用）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lat_lon ON meteorological_data (latitude, longitude);")

        # BRIN インデックス（時系列データに効果的）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time_brin ON meteorological_data USING BRIN (time);")

    return conn


def insert(conn, data):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meteorological_data (time, callsign, altitude, latitude, longitude, "
            "temperature, wind_x, wind_y, wind_angle, wind_speed) "
            "VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                data["callsign"],
                data["altitude"],
                data["latitude"],
                data["longitude"],
                data["temperature"],
                data["wind"]["x"],
                data["wind"]["y"],
                data["wind"]["angle"],
                data["wind"]["speed"],
            ),
        )


def store_queue(conn, measurement_queue):
    logging.info("Start store worker")

    try:
        while True:
            try:
                data = measurement_queue.get(timeout=1)
                insert(conn, data)
            except queue.Empty:
                pass

            if should_terminate.is_set():
                break

    except Exception:
        conn.close()
        logging.exception("Error in store_queue")

    logging.warning("Stop store worker")


def store_term():
    should_terminate.set()


def fetch_by_time(conn, time_start, time_end):
    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM meteorological_data WHERE time BETWEEN %s AND %s",
            (
                time_start.astimezone(datetime.timezone.utc),
                time_end.astimezone(datetime.timezone.utc),
            ),
        )
        data = cur.fetchall()

        logging.info("Elapsed time: %.2f sec", time.perf_counter() - start)

        return data


if __name__ == "__main__":
    import multiprocessing

    import docopt
    import my_lib.config
    import my_lib.logger

    import modes.receiver

    args = docopt.docopt(__doc__)

    my_lib.logger.init("ModeS sensing", level=logging.INFO)

    config_file = args["-c"]
    config = my_lib.config.load(args["-c"])

    measurement_queue = multiprocessing.Queue()

    modes.receiver.start(
        config["modes"]["decoder"]["host"],
        config["modes"]["decoder"]["port"],
        measurement_queue,
        config["filter"]["area"],
    )

    conn = open(
        config["database"]["host"],
        config["database"]["port"],
        config["database"]["name"],
        config["database"]["user"],
        config["database"]["pass"],
    )

    store_queue(conn, measurement_queue)
