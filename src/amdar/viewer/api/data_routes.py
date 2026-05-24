"""DB を直接参照するデータ系 HTTP ハンドラ。

グラフ生成とは独立した、データ範囲取得・マテリアライズドビュー操作・
最終受信時刻取得などのエンドポイント群。
"""

from __future__ import annotations

import datetime
import json
import logging

import flask
import my_lib.time

import amdar.config
import amdar.database.postgresql
from amdar.constants import DEFAULT_DISTANCE_KM

blueprint = flask.Blueprint("modes-sensing-data", __name__)


def _connect_database(config: amdar.config.Config):
    return amdar.database.postgresql.open(
        config.database.host,
        config.database.port,
        config.database.name,
        config.database.user,
        config.database.password,
    )


@blueprint.route("/api/refresh-aggregates", methods=["POST"])
def refresh_aggregates():
    """マテリアライズドビュー（集約データ）を更新する。"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        timings = amdar.database.postgresql.refresh_materialized_views(conn)
        stats = amdar.database.postgresql.get_materialized_view_stats(conn)
        conn.close()

        return flask.jsonify(
            {
                "status": "success",
                "refresh_times": timings.to_dict(),
                "stats": stats.to_dict(),
            }
        )
    except Exception as e:
        logging.exception("Error refreshing materialized views")
        return flask.jsonify({"error": "Failed to refresh views", "details": str(e)}), 500


@blueprint.route("/api/aggregate-stats", methods=["GET"])
def aggregate_stats():
    """マテリアライズドビューの統計情報。"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        exists = amdar.database.postgresql.check_materialized_views_exist(conn)
        stats = amdar.database.postgresql.get_materialized_view_stats(conn)
        conn.close()

        return flask.jsonify({"exists": exists, "stats": stats.to_dict()})
    except Exception as e:
        logging.exception("Error getting aggregate stats")
        return flask.jsonify({"error": "Failed to get stats", "details": str(e)}), 500


@blueprint.route("/api/data-range", methods=["GET"])
def data_range():
    """DB の最古・最新データの日時と件数。"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        result = amdar.database.postgresql.fetch_data_range(conn)
        conn.close()

        if not (result.earliest and result.latest):
            return flask.jsonify({"earliest": None, "latest": None, "count": 0})

        # DB は JST naive datetime で保存。TZ 情報を補う
        earliest = result.earliest
        latest = result.latest
        if earliest.tzinfo is None:
            earliest = earliest.replace(tzinfo=my_lib.time.get_zoneinfo())
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=my_lib.time.get_zoneinfo())

        return flask.jsonify(
            {
                "earliest": earliest.isoformat(),
                "latest": latest.isoformat(),
                "count": result.count,
            }
        )
    except Exception as e:
        logging.exception("Error fetching data range")
        return flask.jsonify({"error": "データ範囲の取得に失敗しました", "details": str(e)}), 500


@blueprint.route("/api/last-received", methods=["GET"])
def last_received():
    """受信方式別の最終受信時刻。"""
    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        result = amdar.database.postgresql.fetch_last_received_by_method(conn)
        conn.close()

        def format_datetime(dt: datetime.datetime | None) -> str | None:
            if dt is None:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=my_lib.time.get_zoneinfo())
            return dt.isoformat()

        return flask.jsonify(
            {
                "mode_s": format_datetime(result.mode_s),
                "vdl2": format_datetime(result.vdl2),
            }
        )
    except Exception as e:
        logging.exception("Error fetching last received times")
        return flask.jsonify({"error": "最終受信時刻の取得に失敗しました", "details": str(e)}), 500


@blueprint.route("/api/debug/date-parse", methods=["GET"])
def debug_date_parse():
    """デバッグ用: 日付パースと集約レベルの推定結果を返す。"""
    time_end_str = flask.request.args.get("end", None)
    time_start_str = flask.request.args.get("start", None)

    result: dict = {
        "raw_params": {"start": time_start_str, "end": time_end_str},
        "parsed": {},
        "aggregation": {},
        "data_sample": {},
    }

    default_time_end = my_lib.time.now()
    default_time_start = default_time_end - datetime.timedelta(days=1)

    if time_end_str:
        try:
            parsed_end = json.loads(time_end_str)
            time_end = datetime.datetime.fromisoformat(parsed_end).astimezone(my_lib.time.get_zoneinfo())
            result["parsed"]["end"] = {
                "json_parsed": parsed_end,
                "datetime": str(time_end),
                "utc": str(time_end.astimezone(datetime.UTC)),
            }
        except (ValueError, TypeError) as e:
            result["parsed"]["end_error"] = str(e)
            time_end = default_time_end
    else:
        time_end = default_time_end
        result["parsed"]["end"] = {"default": str(default_time_end)}

    if time_start_str:
        try:
            parsed_start = json.loads(time_start_str)
            time_start = datetime.datetime.fromisoformat(parsed_start).astimezone(my_lib.time.get_zoneinfo())
            result["parsed"]["start"] = {
                "json_parsed": parsed_start,
                "datetime": str(time_start),
                "utc": str(time_start.astimezone(datetime.UTC)),
            }
        except (ValueError, TypeError) as e:
            result["parsed"]["start_error"] = str(e)
            time_start = default_time_start
    else:
        time_start = default_time_start
        result["parsed"]["start"] = {"default": str(default_time_start)}

    period_days = (time_end - time_start).total_seconds() / 86400
    result["period_days"] = period_days

    level = amdar.database.postgresql.get_aggregation_level(period_days)
    result["aggregation"] = {
        "table": level.table,
        "time_interval": level.time_interval,
        "altitude_bin": level.altitude_bin,
    }

    try:
        config = flask.current_app.config["CONFIG"]
        conn = _connect_database(config)

        view_exists = amdar.database.postgresql.check_materialized_views_exist(conn)
        result["views_exist"] = view_exists

        if period_days > 14:
            raw_data = amdar.database.postgresql.fetch_aggregated_by_time(
                conn, time_start, time_end, max_altitude=None
            )
        else:
            raw_data = amdar.database.postgresql.fetch_by_time(
                conn, time_start, time_end, distance=DEFAULT_DISTANCE_KM
            )

        conn.close()

        if raw_data:
            times = [r["time"] for r in raw_data]
            result["data_sample"] = {
                "total_rows": len(raw_data),
                "min_time": str(min(times)),
                "max_time": str(max(times)),
                "first_3": [
                    {k: str(v) if isinstance(v, datetime.datetime) else v for k, v in row.items()}
                    for row in raw_data[:3]
                ],
            }
        else:
            result["data_sample"] = {"error": "No data returned"}
    except Exception as e:
        result["data_sample"] = {"error": str(e)}

    return flask.jsonify(result)
