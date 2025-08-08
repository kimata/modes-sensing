#!/usr/bin/env python3
"""
Mode S のメッセージを保管し，条件にマッチしたものを出力します．

Usage:
  database_postgres.py [-c CONFIG] [-D]

Options:
  -c CONFIG         : CONFIG を設定ファイルとして読み込んで実行します．[default: config.yaml]
  -D                : デバッグモードで動作します．
"""

import datetime
import logging
import queue
import threading
import time

import my_lib.footprint
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
            "distance REAL, "
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_distance ON meteorological_data (distance);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_altitude ON meteorological_data (altitude);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_latitude ON meteorological_data (latitude);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_longitude ON meteorological_data (longitude);")

        # 複合インデックス（よく使われる組み合わせ）
        # 時刻と距離の組み合わせ（メインクエリ用）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_time_distance ON meteorological_data (time, distance);")
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
            "INSERT INTO meteorological_data (time, callsign, distance, altitude, latitude, longitude, "
            "temperature, wind_x, wind_y, wind_angle, wind_speed) "
            "VALUES (CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                data["callsign"],
                data["distance"],
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


def store_queue(conn, measurement_queue, liveness_file, count=0):
    logging.info("Start store worker")

    i = 0
    try:
        while True:
            try:
                data = measurement_queue.get(timeout=1)
                insert(conn, data)
                my_lib.footprint.update(liveness_file)

                i += 1
                if (count != 0) and (i == count):
                    break
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


def fetch_by_time(conn, time_start, time_end, distance, columns=None):
    """
    指定された時間範囲と距離でデータを取得する

    Args:
        conn: データベース接続
        time_start: 開始時刻
        time_end: 終了時刻
        distance: 距離フィルタ
        columns: 取得するカラムのリスト。Noneの場合はデフォルト['time', 'altitude', 'temperature', 'distance']

    Returns:
        取得されたデータのリスト

    """
    if columns is None:
        columns = ["time", "altitude", "temperature", "distance"]

    # カラム名をサニタイズ（SQLインジェクション対策）
    valid_columns = [
        "time",
        "callsign",
        "distance",
        "altitude",
        "latitude",
        "longitude",
        "temperature",
        "wind_x",
        "wind_y",
        "wind_angle",
        "wind_speed",
    ]
    sanitized_columns = [col for col in columns if col in valid_columns]

    if not sanitized_columns:
        msg = "No valid columns specified"
        raise ValueError(msg)

    columns_str = ", ".join(sanitized_columns)

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # クエリを最適化：インデックスを効率的に使用
        query = (
            f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
            f"WHERE time >= %s AND time <= %s AND distance <= %s "
            f"ORDER BY time"
        )
        cur.execute(
            query,
            (
                time_start.astimezone(datetime.timezone.utc),
                time_end.astimezone(datetime.timezone.utc),
                distance,
            ),
        )
        # fetchallではなく大きなデータセット向けにitersize指定でメモリ効率化
        cur.itersize = 10000  # 大量データ取得時のメモリ効率化
        data = cur.fetchall()

        logging.info(
            "Elapsed time: %.2f sec (selected %d columns, %s rows)",
            time.perf_counter() - start,
            len(sanitized_columns),
            f"{len(data):,}",
        )

        return data


def fetch_latest(conn, limit, distance=None, columns=None):
    """
    最新のデータを指定された件数取得する

    Args:
        conn: データベース接続
        limit: 取得する最大件数
        distance: 距離フィルタ（Noneの場合はフィルタなし）
        columns: 取得するカラムのリスト。Noneの場合はデフォルト['time', 'altitude', 'temperature', 'distance']

    Returns:
        取得されたデータのリスト（時間の降順でソート）

    """
    if columns is None:
        columns = ["time", "altitude", "temperature", "distance"]

    # カラム名をサニタイズ（SQLインジェクション対策）
    valid_columns = [
        "time",
        "callsign",
        "distance",
        "altitude",
        "latitude",
        "longitude",
        "temperature",
        "wind_x",
        "wind_y",
        "wind_angle",
        "wind_speed",
    ]
    sanitized_columns = [col for col in columns if col in valid_columns]

    if not sanitized_columns:
        msg = "No valid columns specified"
        raise ValueError(msg)

    columns_str = ", ".join(sanitized_columns)

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 距離フィルタの有無で条件分岐
        if distance is not None:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
                f"AND temperature > -100 AND distance <= %s "
                f"ORDER BY time DESC LIMIT %s"
            )
            cur.execute(query, (distance, limit))
        else:
            query = (
                f"SELECT {columns_str} FROM meteorological_data "  # noqa: S608
                f"WHERE altitude IS NOT NULL AND temperature IS NOT NULL "
                f"AND temperature > -100 "
                f"ORDER BY time DESC LIMIT %s"
            )
            cur.execute(query, (limit,))

        data = cur.fetchall()

        logging.info(
            "Elapsed time: %.2f sec (selected %d columns, %s rows)",
            time.perf_counter() - start,
            len(sanitized_columns),
            f"{len(data):,}",
        )

        return data


def fetch_data_range(conn):
    """
    データベースの最古・最新データの日時とレコード数を取得する

    Args:
        conn: データベース接続

    Returns:
        dict: earliest, latest, countを含む辞書

    """
    query = """
    SELECT
        MIN(time) as earliest,
        MAX(time) as latest,
        COUNT(*) as count
    FROM meteorological_data
    """

    start = time.perf_counter()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        result = cur.fetchone()

    logging.info(
        "Elapsed time: %.2f sec (data range query)",
        time.perf_counter() - start,
    )

    if result and result["earliest"] and result["latest"]:
        return {
            "earliest": result["earliest"],
            "latest": result["latest"],
            "count": result["count"],
        }
    else:
        # データがない場合
        return {"earliest": None, "latest": None, "count": 0}


if __name__ == "__main__":
    import multiprocessing

    import docopt
    import my_lib.config
    import my_lib.logger

    import modes.receiver

    args = docopt.docopt(__doc__)

    config_file = args["-c"]
    debug_mode = args["-D"]

    my_lib.logger.init("modes-sensing", level=logging.DEBUG if debug_mode else logging.INFO)

    config = my_lib.config.load(config_file)

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

    store_queue(conn, measurement_queue, config["liveness"]["file"]["collector"])
