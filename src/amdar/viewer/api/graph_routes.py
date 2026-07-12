"""グラフ生成 HTTP エンドポイント。

すべての処理は :data:`amdar.viewer.graph.service.graph_service` に委譲し、
本モジュールはリクエスト/レスポンスの整形だけを担う。
"""

from __future__ import annotations

import datetime
import functools
import json
import logging
import time
from typing import Any, cast

import flask
import my_lib.time
import PIL.Image
import PIL.ImageDraw
import PIL.ImageFont

import amdar.config
from amdar.constants import (
    CACHE_CONTROL_MAX_AGE_RESULT,
    GRAPH_JOB_MAX_GRAPHS,
    SSE_MAX_CONNECTION_SECONDS,
    SSE_MAX_JOB_IDS,
    SSE_POLL_INTERVAL_SECONDS,
    GraphName,
)
from amdar.viewer.api.job_manager import JobStatus, JobStatusDict, job_manager
from amdar.viewer.graph import cache
from amdar.viewer.graph.definitions import GRAPH_DEF_MAP
from amdar.viewer.graph.service import graph_service

blueprint = flask.Blueprint("modes-sensing-graph", __name__)

# SSE で「終端」とみなすステータス
_SSE_TERMINAL_STATUSES = frozenset({"completed", "failed", "timeout", "unknown"})

# エラー画像のデフォルトサイズ（px）
_ERROR_IMAGE_SIZE = (1200, 800)
_ERROR_IMAGE_FONT_SIZE = 28


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

    try:
        time_end = _parse_json_datetime(time_end_str, default_time_end)
        time_start = _parse_json_datetime(time_start_str, default_time_start)
    except ValueError as e:
        return flask.jsonify({"error": str(e)}), 400
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
    """JSON エンコードされた ISO 形式の日時をパース。

    パラメータ未指定時はデフォルト値を返す。パース失敗時は ValueError を送出する
    （黙ってデフォルトにフォールバックしない）。
    """
    if not date_str:
        return default
    try:
        parsed = json.loads(date_str)
        return datetime.datetime.fromisoformat(parsed).astimezone(my_lib.time.get_zoneinfo())
    except (ValueError, TypeError) as e:
        logging.debug("Failed to parse datetime: %s", date_str, exc_info=True)
        msg = f"Invalid datetime parameter: {date_str}"
        raise ValueError(msg) from e


def _get_error_font_path(config: amdar.config.Config | None) -> str | None:
    """エラー画像描画に使うフォントファイルのパスを返す（無ければ None）。"""
    if config is None:
        return None
    try:
        font_file = config.font.map.get("jp_bold") or next(iter(config.font.map.values()), None)
        if font_file is None:
            return None
        font_path = config.font.path / font_file
        if font_path.exists():
            return str(font_path)
    except (OSError, TypeError, AttributeError):
        logging.debug("Failed to resolve error image font", exc_info=True)
    return None


@functools.lru_cache(maxsize=32)
def _render_error_image(message: str, size: tuple[int, int], font_path: str | None) -> bytes:
    """Pillow でエラー画像（グレー背景 + メッセージ）を生成する。

    matplotlib はスレッド安全でないため、リクエストスレッドで動く本関数では使わない。
    生成結果はメッセージ・サイズ・フォントをキーにキャッシュされる。
    """
    import io

    img = PIL.Image.new("RGB", size, color=(230, 230, 230))
    draw = PIL.ImageDraw.Draw(img)

    font: PIL.ImageFont.FreeTypeFont | PIL.ImageFont.ImageFont
    if font_path is not None:
        try:
            font = PIL.ImageFont.truetype(font_path, _ERROR_IMAGE_FONT_SIZE)
        except OSError:
            font = PIL.ImageFont.load_default()
    else:
        font = PIL.ImageFont.load_default()

    bbox = draw.multiline_textbbox((0, 0), message, font=font, align="center")
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    pos = ((size[0] - text_width) / 2 - bbox[0], (size[1] - text_height) / 2 - bbox[1])
    draw.multiline_text(pos, message, font=font, fill=(80, 80, 80), align="center")

    buf = io.BytesIO()
    img.save(buf, "PNG")
    return buf.getvalue()


def _error_response(graph_name: str, error_text: str) -> flask.Response:
    """エラー時に薄い PNG レスポンスを返す（ブラウザに固まらないように）。"""
    config = flask.current_app.config.get("CONFIG")
    message = f"Graph generation failed\n{graph_name}\nError: {error_text[:100]}"
    error_image_bytes = _render_error_image(message, _ERROR_IMAGE_SIZE, _get_error_font_path(config))

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

        if time_start >= time_end:
            return flask.jsonify({"error": "start must be before end"}), 400

        graphs = data.get("graphs", [])
        if not graphs or not isinstance(graphs, list):
            return flask.jsonify({"error": "graphs list is required"}), 400

        # 重複を除去（順序は維持）
        unique_graphs = list(dict.fromkeys(graphs))

        if len(unique_graphs) > GRAPH_JOB_MAX_GRAPHS:
            return (
                flask.jsonify({"error": f"Too many graphs (max: {GRAPH_JOB_MAX_GRAPHS})"}),
                400,
            )

        invalid_graphs = [name for name in unique_graphs if name not in GRAPH_DEF_MAP]
        if invalid_graphs:
            return flask.jsonify({"error": f"Unknown graph names: {invalid_graphs}"}), 400

        jobs = []
        for graph_name in unique_graphs:
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
    status_dict = job_manager.get_job_status_dict(job_id)
    if not status_dict:
        return flask.jsonify({"error": "Job not found"}), 404
    return flask.jsonify(status_dict)


def _collect_jobs_status(job_ids: list[str]) -> dict[str, dict[str, Any]]:
    """複数ジョブのステータスを取得する。

    不明な job_id も ``{"status": "unknown"}`` として応答に含める。
    """
    results: dict[str, dict[str, Any]] = {}
    for job_id in job_ids:
        status_dict = job_manager.get_job_status_dict(job_id)
        if status_dict:
            results[job_id] = {
                "status": status_dict["status"],
                "progress": status_dict["progress"],
                "graph_name": status_dict["graph_name"],
                "error": status_dict["error"],
                "elapsed_seconds": status_dict["elapsed_seconds"],
                "stage": status_dict["stage"],
            }
        else:
            results[job_id] = {"status": "unknown"}
    return results


@blueprint.route("/api/graph/jobs/status", methods=["GET", "POST"])
def get_jobs_status_batch():
    """複数ジョブのステータスを一括取得（ポーリング効率化）。

    - GET: ``?job_ids=<カンマ区切り>``
    - POST: ``{"job_ids": [...]}``（旧形式）

    不明な job_id も ``{"status": "unknown"}`` として応答に含める。
    """
    try:
        if flask.request.method == "GET":
            job_ids_param = flask.request.args.get("job_ids", "")
            job_ids = [job_id for job_id in (s.strip() for s in job_ids_param.split(",")) if job_id]
            if not job_ids:
                return flask.jsonify({"error": "job_ids is required"}), 400
        else:
            data = flask.request.get_json()
            if not data:
                return flask.jsonify({"error": "Request body is required"}), 400
            job_ids = data.get("job_ids", [])
            if not isinstance(job_ids, list):
                return flask.jsonify({"error": "job_ids must be a list"}), 400

        return flask.jsonify({"jobs": _collect_jobs_status(job_ids)})

    except Exception as e:
        logging.exception("Error getting jobs status")
        return flask.jsonify({"error": str(e)}), 500


@blueprint.route("/api/graph/job/events", methods=["GET"])
def job_events():
    """ジョブステータスの Server-Sent Events ストリーム。

    ``?job_ids=<カンマ区切り>`` で監視対象を指定する。

    - 接続直後に ``event: status`` で全ジョブのスナップショットを送信
    - 以降は変化があったときのみ同形式の ``event: status`` を送信
    - 全ジョブが終端状態（completed/failed/timeout/unknown）になったら
      ``event: done`` を送ってストリームを閉じる
    """
    job_ids_param = flask.request.args.get("job_ids", "")
    job_ids = [job_id for job_id in (s.strip() for s in job_ids_param.split(",")) if job_id]

    if not job_ids:
        return flask.jsonify({"error": "job_ids is required"}), 400
    if len(job_ids) > SSE_MAX_JOB_IDS:
        return flask.jsonify({"error": f"Too many job_ids (max: {SSE_MAX_JOB_IDS})"}), 400

    def snapshot() -> dict[str, JobStatusDict | dict[str, str]]:
        return {
            job_id: (job_manager.get_job_status_dict(job_id) or {"status": "unknown"}) for job_id in job_ids
        }

    def significant(state: dict[str, JobStatusDict | dict[str, str]]) -> tuple:
        """変化検知用のキー（elapsed_seconds のような常時変動する値は除外）。"""
        return tuple(
            (
                job_id,
                status.get("status"),
                status.get("progress"),
                status.get("stage"),
                status.get("error"),
            )
            for job_id, status in state.items()
        )

    def all_terminal(state: dict[str, JobStatusDict | dict[str, str]]) -> bool:
        return all(status.get("status") in _SSE_TERMINAL_STATUSES for status in state.values())

    def format_status_event(state: dict[str, JobStatusDict | dict[str, str]]) -> str:
        return f"event: status\ndata: {json.dumps({'jobs': state})}\n\n"

    def generate():
        deadline = time.time() + SSE_MAX_CONNECTION_SECONDS

        state = snapshot()
        last_key = significant(state)
        yield format_status_event(state)

        if all_terminal(state):
            yield "event: done\ndata: {}\n\n"
            return

        while time.time() < deadline:
            time.sleep(SSE_POLL_INTERVAL_SECONDS)
            state = snapshot()
            key = significant(state)
            if key != last_key:
                last_key = key
                yield format_status_event(state)
            if all_terminal(state):
                yield "event: done\ndata: {}\n\n"
                return

        logging.info("SSE connection reached max duration, closing")

    res = flask.Response(
        flask.stream_with_context(generate()),
        mimetype="text/event-stream",
    )
    res.headers["Cache-Control"] = "no-cache"
    res.headers["X-Accel-Buffering"] = "no"
    return res


@blueprint.route("/api/graph/job/<job_id>/result", methods=["GET"])
def get_job_result(job_id: str):
    """ジョブ結果（PNG 画像）。"""
    job = job_manager.get_job(job_id)

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
    res.headers["Cache-Control"] = f"private, max-age={CACHE_CONTROL_MAX_AGE_RESULT}"
    return res


@blueprint.route("/api/graph/jobs/stats", methods=["GET"])
def get_jobs_stats():
    """ジョブ統計情報（デバッグ用）。"""
    stats = job_manager.get_stats()
    return flask.jsonify(stats)
