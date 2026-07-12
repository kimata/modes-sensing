"""DB を直接参照するデータ系 HTTP ハンドラ。

グラフ生成とは独立した、データ範囲取得・マテリアライズドビュー操作・
最終受信時刻取得などのエンドポイント群。
"""

from __future__ import annotations

import contextlib
import datetime
import json
import logging
import threading
import time

import flask
import my_lib.time

import amdar.config
import amdar.database.postgresql
from amdar.constants import DEFAULT_DISTANCE_KM, REFRESH_AGGREGATES_MIN_INTERVAL_SECONDS
from amdar.viewer.api.job_manager import JobStatus, job_manager

blueprint = flask.Blueprint("modes-sensing-data", __name__)

# Prometheus text exposition format (version 0.0.4) の Content-Type
_PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

# POST /api/refresh-aggregates のレート制限用状態
_refresh_rate_lock = threading.Lock()
_last_refresh_time: float = 0.0


def _connect_database(config: amdar.config.Config):
    # スキーマ適用（DDL）は収集側で実施済みのため、閲覧側の接続では省略する
    return amdar.database.postgresql.open(
        config.database.host,
        config.database.port,
        config.database.name,
        config.database.user,
        config.database.password,
        apply_schema=False,
    )


@blueprint.route("/api/refresh-aggregates", methods=["POST"])
def refresh_aggregates():
    """マテリアライズドビュー（集約データ）を更新する。

    負荷の高い操作のため、最終実行から一定時間はレート制限（429）する。
    """
    global _last_refresh_time

    with _refresh_rate_lock:
        now = time.time()
        remaining = REFRESH_AGGREGATES_MIN_INTERVAL_SECONDS - (now - _last_refresh_time)
        if remaining > 0:
            return (
                flask.jsonify(
                    {
                        "error": "Too many requests",
                        "retry_after_seconds": int(remaining) + 1,
                    }
                ),
                429,
            )
        _last_refresh_time = now

    try:
        config = flask.current_app.config["CONFIG"]
        with contextlib.closing(_connect_database(config)) as conn:
            timings = amdar.database.postgresql.refresh_materialized_views(conn)
            stats = amdar.database.postgresql.get_materialized_view_stats(conn)

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
        with contextlib.closing(_connect_database(config)) as conn:
            exists = amdar.database.postgresql.check_materialized_views_exist(conn)
            stats = amdar.database.postgresql.get_materialized_view_stats(conn)

        return flask.jsonify({"exists": exists, "stats": stats.to_dict()})
    except Exception as e:
        logging.exception("Error getting aggregate stats")
        return flask.jsonify({"error": "Failed to get stats", "details": str(e)}), 500


@blueprint.route("/api/data-range", methods=["GET"])
def data_range():
    """DB の最古・最新データの日時と件数。"""
    try:
        config = flask.current_app.config["CONFIG"]
        with contextlib.closing(_connect_database(config)) as conn:
            result = amdar.database.postgresql.fetch_data_range(conn)

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
        with contextlib.closing(_connect_database(config)) as conn:
            result = amdar.database.postgresql.fetch_last_received_by_method(conn)

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


def _format_datetime_iso(dt: datetime.datetime | None) -> str | None:
    """datetime を ISO 形式文字列にする（naive の場合は JST を補う）。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=my_lib.time.get_zoneinfo())
    return dt.isoformat()


def _age_seconds(dt: datetime.datetime | None) -> float | None:
    """最終観測時刻からの経過秒数を返す（未観測の場合は None）。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=my_lib.time.get_zoneinfo())
    return max(0.0, (my_lib.time.now() - dt).total_seconds())


def _count_cache_files(config: amdar.config.Config) -> int:
    """グラフキャッシュディレクトリ内の PNG ファイル数を返す。"""
    cache_dir = config.webapp.cache_dir_path
    if not cache_dir.exists():
        return 0
    return sum(1 for _ in cache_dir.glob("*.png"))


def _fetch_receiver_quality(config: amdar.config.Config) -> amdar.database.postgresql.ReceiverQualityResult:
    """受信品質スナップショットを取得する（DB 側で 60 秒 TTL キャッシュ）。"""
    with contextlib.closing(_connect_database(config)) as conn:
        return amdar.database.postgresql.fetch_receiver_quality(conn)


@blueprint.route("/api/receiver-quality", methods=["GET"])
def receiver_quality():
    """受信品質（方式別の観測数・最終受信時刻・集約テーブル行数）を返す。"""
    try:
        config = flask.current_app.config["CONFIG"]
        quality = _fetch_receiver_quality(config)

        return flask.jsonify(
            {
                "mode_s": {
                    "last_hour": quality.last_hour.mode_s,
                    "last_24h": quality.last_24h.mode_s,
                    "last_received": _format_datetime_iso(quality.last_received.mode_s),
                    "age_seconds": _age_seconds(quality.last_received.mode_s),
                },
                "vdl2": {
                    "last_hour": quality.last_hour.vdl2,
                    "last_24h": quality.last_24h.vdl2,
                    "last_received": _format_datetime_iso(quality.last_received.vdl2),
                    "age_seconds": _age_seconds(quality.last_received.vdl2),
                },
                "aggregates": quality.aggregate_rows.to_dict(),
            }
        )
    except Exception as e:
        logging.exception("Error fetching receiver quality")
        return flask.jsonify({"error": "受信品質の取得に失敗しました", "details": str(e)}), 500


def _build_metrics_text(
    quality: amdar.database.postgresql.ReceiverQualityResult,
    job_stats: dict[str, int],
    cache_files: int,
) -> str:
    """Prometheus text format (version 0.0.4) のメトリクス本文を構築する。"""
    lines: list[str] = []

    lines += [
        "# HELP modes_sensing_observations_total Number of observations in the last 24 hours",
        "# TYPE modes_sensing_observations_total gauge",
        f'modes_sensing_observations_total{{method="mode-s"}} {quality.last_24h.mode_s}',
        f'modes_sensing_observations_total{{method="vdl2"}} {quality.last_24h.vdl2}',
        "# HELP modes_sensing_observations_last_hour Number of observations in the last hour",
        "# TYPE modes_sensing_observations_last_hour gauge",
        f'modes_sensing_observations_last_hour{{method="mode-s"}} {quality.last_hour.mode_s}',
        f'modes_sensing_observations_last_hour{{method="vdl2"}} {quality.last_hour.vdl2}',
    ]

    lines += [
        "# HELP modes_sensing_last_observation_age_seconds Seconds since the last observation",
        "# TYPE modes_sensing_last_observation_age_seconds gauge",
    ]
    for method_label, last_received in (
        ("mode-s", quality.last_received.mode_s),
        ("vdl2", quality.last_received.vdl2),
    ):
        age = _age_seconds(last_received)
        if age is not None:
            lines.append(f'modes_sensing_last_observation_age_seconds{{method="{method_label}"}} {age:.0f}')

    lines += [
        "# HELP modes_sensing_aggregate_rows Number of rows in aggregate tables",
        "# TYPE modes_sensing_aggregate_rows gauge",
    ]
    for table, row_count in quality.aggregate_rows.to_dict().items():
        lines.append(f'modes_sensing_aggregate_rows{{table="{table}"}} {row_count}')

    lines += [
        "# HELP modes_sensing_jobs Number of graph generation jobs by status",
        "# TYPE modes_sensing_jobs gauge",
    ]
    for status in JobStatus:
        lines.append(f'modes_sensing_jobs{{status="{status.value}"}} {job_stats.get(status.value, 0)}')

    lines += [
        "# HELP modes_sensing_cache_files Number of cached graph PNG files",
        "# TYPE modes_sensing_cache_files gauge",
        f"modes_sensing_cache_files {cache_files}",
    ]

    return "\n".join(lines) + "\n"


@blueprint.route("/api/metrics", methods=["GET"])
def metrics():
    """Prometheus text format (version 0.0.4) の受信品質メトリクス。"""
    try:
        config = flask.current_app.config["CONFIG"]
        quality = _fetch_receiver_quality(config)
        body = _build_metrics_text(quality, job_manager.get_stats(), _count_cache_files(config))
        return flask.Response(body, content_type=_PROMETHEUS_CONTENT_TYPE)
    except Exception:
        logging.exception("Error generating metrics")
        return flask.Response(
            "# metrics collection failed\n",
            status=500,
            content_type=_PROMETHEUS_CONTENT_TYPE,
        )


@blueprint.route("/api/debug/date-parse", methods=["GET"])
def debug_date_parse():
    """デバッグ用: 日付パースと集約レベルの推定結果を返す。

    Flask の debug モード時のみ有効（非 debug では 404）。
    """
    if not flask.current_app.debug:
        return flask.jsonify({"error": "Not found"}), 404

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
        with contextlib.closing(_connect_database(config)) as conn:
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
