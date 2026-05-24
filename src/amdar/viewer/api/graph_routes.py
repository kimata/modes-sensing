"""グラフ生成 HTTP エンドポイント。

すべての処理は :data:`amdar.viewer.graph.service.graph_service` に委譲し、
本モジュールはリクエスト/レスポンスの整形だけを担う。
"""

from __future__ import annotations

import datetime
import json
import logging
from typing import Any, cast

import flask
import my_lib.time

from amdar.constants import (
    CACHE_CONTROL_MAX_AGE_RESULT,
    CACHE_CONTROL_MAX_AGE_STATUS,
    GraphName,
)
from amdar.viewer.api.job_manager import JobManager, JobStatus
from amdar.viewer.graph import cache
from amdar.viewer.graph.definitions import GRAPH_DEF_MAP
from amdar.viewer.graph.service import graph_service

# JobManager もシングルトンなので routes から直接参照してよい
_job_manager = JobManager()

blueprint = flask.Blueprint("modes-sensing-graph", __name__)


def _parse_iso_datetime(date_str: str | None) -> datetime.datetime | None:
    """ISO 形式の日時を JST に変換してパース。失敗時は None。"""
    if not date_str:
        return None
    try:
        return datetime.datetime.fromisoformat(date_str).astimezone(my_lib.time.get_zoneinfo())
    except (ValueError, TypeError):
        logging.exception("Failed to parse datetime: %s", date_str)
        return None


@blueprint.route("/api/graph/<path:graph_name>", methods=["GET"])
def graph(graph_name: str):
    """グラフ画像を同期取得（古い API、ETag 付き）。

    クエリ ``start``, ``end``: JSON 文字列としての ISO 形式日時。
    クエリ ``limit_altitude``: ``true`` で高度制限あり。
    """
    default_time_end = my_lib.time.now()
    default_time_start = default_time_end - datetime.timedelta(days=1)

    time_end_str = flask.request.args.get("end", None)
    time_start_str = flask.request.args.get("start", None)
    limit_altitude_str = flask.request.args.get("limit_altitude", "false")

    time_end = _parse_json_datetime(time_end_str, default_time_end)
    time_start = _parse_json_datetime(time_start_str, default_time_start)
    limit_altitude = limit_altitude_str.lower() == "true"

    request_days = (time_end - time_start).total_seconds() / 86400
    logging.info(
        "request: %s graph (start: %s, end: %s, limit_altitude: %s, period: %.2f days)",
        graph_name,
        time_start,
        time_end,
        limit_altitude,
        request_days,
    )

    if graph_name not in GRAPH_DEF_MAP:
        return flask.jsonify({"error": f"Unknown graph: {graph_name}"}), 400
    # GRAPH_DEF_MAP に含まれることを上で確認したので GraphName に絞り込める
    typed_graph_name = cast(GraphName, graph_name)

    etag_key = cache.generate_etag_key(typed_graph_name, time_start, time_end, limit_altitude)
    etag = f'"{etag_key}"'

    if_none_match = flask.request.headers.get("If-None-Match")
    if if_none_match and if_none_match == etag:
        logging.info("Returning 304 Not Modified for %s (ETag matched)", typed_graph_name)
        return flask.Response(status=304, headers={"ETag": etag})

    try:
        image_bytes = graph_service.generate_sync(typed_graph_name, time_start, time_end, limit_altitude)

        res = flask.Response(image_bytes, mimetype="image/png")
        res.headers["Cache-Control"] = f"private, max-age={CACHE_CONTROL_MAX_AGE_RESULT}"
        res.headers["ETag"] = etag
        res.headers["X-Content-Type-Options"] = "nosniff"
        return res
    except Exception as e:
        logging.exception("Error generating graph %s", graph_name)
        return _error_response(graph_name, str(e))


def _parse_json_datetime(date_str: str | None, default: datetime.datetime) -> datetime.datetime:
    """JSON エンコードされた ISO 形式の日時をパース。失敗時はデフォルト。"""
    if not date_str:
        return default
    try:
        parsed = json.loads(date_str)
        return datetime.datetime.fromisoformat(parsed).astimezone(my_lib.time.get_zoneinfo())
    except (ValueError, TypeError):
        logging.debug("Failed to parse datetime", exc_info=True)
        return default


def _error_response(graph_name: str, error_text: str) -> flask.Response:
    """エラー時に薄い PNG レスポンスを返す（ブラウザに固まらないように）。"""
    import io

    import matplotlib.pyplot as plt

    from amdar.constants import GRAPH_IMAGE_DPI

    fig, ax = plt.subplots(figsize=(12, 8))
    ax.text(
        0.5,
        0.5,
        f"Graph generation failed\n{graph_name}\nError: {error_text[:100]}...",
        ha="center",
        va="center",
        transform=ax.transAxes,
        fontsize=14,
        bbox={"boxstyle": "round,pad=0.3", "facecolor": "lightcoral", "alpha": 0.7},
    )
    ax.axis("off")

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=GRAPH_IMAGE_DPI, bbox_inches="tight", facecolor="white")
    buf.seek(0)
    error_image_bytes = buf.read()
    buf.close()
    plt.close(fig)

    res = flask.Response(error_image_bytes, mimetype="image/png")
    # エラー時はキャッシュ無効化
    res.headers["Cache-Control"] = "private, no-cache, no-store, must-revalidate, max-age=0"
    res.headers["Pragma"] = "no-cache"
    res.headers["Expires"] = "0"
    res.headers["Vary"] = "Accept, Accept-Encoding"
    res.headers["X-Content-Type-Options"] = "nosniff"
    return res


# =============================================================================
# 非同期グラフ生成 API
# =============================================================================


@blueprint.route("/api/graph/job", methods=["POST"])
def create_graph_job():
    """複数のグラフ生成ジョブを登録する。

    Request Body::

        {
            "graphs": ["scatter_2d", "contour_2d", ...],
            "start": "2025-01-01T00:00:00Z",
            "end": "2025-01-07T00:00:00Z",
            "limit_altitude": false
        }

    Response::

        {"jobs": [{"job_id": "uuid-1", "graph_name": "scatter_2d"}, ...]}
    """
    try:
        data = flask.request.get_json()
        if not data:
            return flask.jsonify({"error": "Request body is required"}), 400

        time_start = _parse_iso_datetime(data.get("start"))
        time_end = _parse_iso_datetime(data.get("end"))
        limit_altitude = data.get("limit_altitude", False)

        if not time_start or not time_end:
            return flask.jsonify({"error": "start and end are required"}), 400

        graphs = data.get("graphs", [])
        if not graphs:
            return flask.jsonify({"error": "graphs list is required"}), 400

        jobs = []
        for graph_name in graphs:
            if graph_name not in GRAPH_DEF_MAP:
                logging.warning("Unknown graph name: %s", graph_name)
                continue
            typed_name = cast(GraphName, graph_name)
            job_id = graph_service.submit_async(typed_name, time_start, time_end, limit_altitude)
            jobs.append({"job_id": job_id, "graph_name": typed_name})

        return flask.jsonify({"jobs": jobs})

    except Exception as e:
        logging.exception("Error creating graph jobs")
        return flask.jsonify({"error": str(e)}), 500


@blueprint.route("/api/graph/job/<job_id>/status", methods=["GET"])
def get_job_status(job_id: str):
    """単一ジョブのステータス。"""
    status_dict = _job_manager.get_job_status_dict(job_id)
    if not status_dict:
        return flask.jsonify({"error": "Job not found"}), 404
    return flask.jsonify(status_dict)


@blueprint.route("/api/graph/jobs/status", methods=["POST"])
def get_jobs_status_batch():
    """複数ジョブのステータスを一括取得（ポーリング効率化）。"""
    try:
        data = flask.request.get_json()
        if not data:
            return flask.jsonify({"error": "Request body is required"}), 400

        job_ids = data.get("job_ids", [])
        results: dict[str, dict[str, Any]] = {}

        for job_id in job_ids:
            status_dict = _job_manager.get_job_status_dict(job_id)
            if status_dict:
                results[job_id] = {
                    "status": status_dict["status"],
                    "progress": status_dict["progress"],
                    "graph_name": status_dict["graph_name"],
                    "error": status_dict["error"],
                    "elapsed_seconds": status_dict["elapsed_seconds"],
                    "stage": status_dict["stage"],
                }

        return flask.jsonify({"jobs": results})

    except Exception as e:
        logging.exception("Error getting jobs status")
        return flask.jsonify({"error": str(e)}), 500


@blueprint.route("/api/graph/job/<job_id>/result", methods=["GET"])
def get_job_result(job_id: str):
    """ジョブ結果（PNG 画像）。"""
    job = _job_manager.get_job(job_id)

    if not job:
        return flask.jsonify({"error": "Job not found"}), 404

    if job.status in {JobStatus.PENDING, JobStatus.PROCESSING}:
        return (
            flask.jsonify(
                {"error": "Job not completed", "status": job.status.value, "progress": job.progress}
            ),
            202,
        )

    if job.status in {JobStatus.FAILED, JobStatus.TIMEOUT}:
        return (
            flask.jsonify({"error": job.error or "Job failed", "status": job.status.value}),
            500,
        )

    if not job.result:
        return flask.jsonify({"error": "No result available"}), 500

    res = flask.Response(job.result, mimetype="image/png")
    res.headers["Cache-Control"] = f"private, max-age={CACHE_CONTROL_MAX_AGE_STATUS}"
    return res


@blueprint.route("/api/graph/jobs/stats", methods=["GET"])
def get_jobs_stats():
    """ジョブ統計情報（デバッグ用）。"""
    stats = _job_manager.get_stats()
    return flask.jsonify(stats)
