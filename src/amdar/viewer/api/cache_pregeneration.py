"""キャッシュ事前生成スケジューラ。

デフォルト7日間表示用のグラフを定期的に自動生成し、キャッシュが常に
新鮮な状態を維持する。

実装上のポイント:
    - グラフ生成は :class:`amdar.viewer.graph.service.GraphService` に委譲。
      matplotlib はサブプロセスで実行されるため、タイマースレッドから安全に呼べる。
    - 期間の終端はフロントエンドのクランプ処理（data-range の latest への吸着）と
      一致させるため、DB の最新データ時刻を使う（現在時刻はフォールバック）。
    - 次回実行は前回実行の「開始」から一定間隔でスケジュールする（固定レート）。
      完了基準にすると生成時間の分だけアンカー間隔が延び、キャッシュヒット判定の
      開始日時許容差を超えるおそれがあるため。
    - キャッシュが TTL の残り時間で十分カバーできる場合は再生成をスキップする。
    - stop() 後は再スケジュールされず、再度 initialize() することで再開できる。

アプリ全体ではモジュールレベルの :data:`cache_pregenerator` インスタンスを共有する。
"""

from __future__ import annotations

import contextlib
import datetime
import logging
import threading
import time

import my_lib.time

import amdar.config
import amdar.database.postgresql
from amdar.constants import (
    DEFAULT_PREGENERATION_DAYS,
    PREGENERATION_INTERVAL_SECONDS,
    GraphName,
)
from amdar.viewer.graph.service import graph_service

# 事前生成対象のグラフ（デフォルト表示で使う全種）
_PREGENERATION_GRAPHS: list[GraphName] = [
    "scatter_2d",
    "contour_2d",
    "density",
    "heatmap",
    "temperature",
    "wind_direction",
    "temperature_profile",
    "hodograph",
    "scatter_3d",
    "contour_3d",
]

# 初回実行までの遅延（アプリ起動完了を待つ）
_INITIAL_DELAY_SECONDS = 10

# 固定レートスケジュールで生成が間隔を超過した場合の最小遅延
_MIN_RESCHEDULE_DELAY_SECONDS = 60


class CachePregenerator:
    """キャッシュ事前生成スケジューラ。"""

    def __init__(self) -> None:
        self._config: amdar.config.Config | None = None
        self._timer: threading.Timer | None = None
        self._running = False
        self._initialized = False
        self._stop_requested = False
        self._lock = threading.Lock()

    def initialize(self, config: amdar.config.Config | None = None) -> None:
        """事前生成を開始する（多重呼び出しは無視、stop() 後の再開は可能）。

        Args:
            config: DB 接続設定。None の場合は期間終端に現在時刻を使う。

        前提: :func:`amdar.viewer.graph.service.graph_service.initialize` が
        事前に呼ばれていること。
        """
        with self._lock:
            if self._initialized:
                return
            self._config = config
            self._initialized = True
            self._stop_requested = False
            self._schedule_next_locked(delay=_INITIAL_DELAY_SECONDS)
            logging.info(
                "CachePregenerator initialized: interval=%d sec, graphs=%d",
                PREGENERATION_INTERVAL_SECONDS,
                len(_PREGENERATION_GRAPHS),
            )

    def stop(self) -> None:
        """事前生成を停止する。

        実行中でも、完了後の再スケジュールは行われない。
        """
        with self._lock:
            self._stop_requested = True
            self._initialized = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
            logging.info("CachePregenerator stopped")

    @property
    def is_running(self) -> bool:
        """事前生成が実行中かどうか。"""
        return self._running

    # ------------------------------------------------------------------
    # 内部メソッド
    # ------------------------------------------------------------------

    def _schedule_next_locked(self, delay: float | None = None) -> None:
        """次回の事前生成をスケジュールする（呼び出し側で _lock を保持していること）。"""
        if self._stop_requested or not self._initialized:
            return
        interval = delay if delay is not None else PREGENERATION_INTERVAL_SECONDS
        self._timer = threading.Timer(interval, self._run_pregeneration)
        self._timer.daemon = True
        self._timer.start()

    def _run_pregeneration(self) -> None:
        start_time = time.perf_counter()
        try:
            self._running = True

            time_end = self._resolve_time_end()
            time_start = time_end - datetime.timedelta(days=DEFAULT_PREGENERATION_DAYS)

            logging.info(
                "[PREGEN] Starting pregeneration: %s to %s",
                time_start.isoformat(),
                time_end.isoformat(),
            )

            generated = self._generate_graphs(time_start, time_end, limit_altitude=False)

            elapsed = time.perf_counter() - start_time
            logging.info(
                "[PREGEN] Completed: %d/%d graphs in %.1f sec",
                generated,
                len(_PREGENERATION_GRAPHS),
                elapsed,
            )
        except Exception:
            logging.exception("[PREGEN] Error during pregeneration")
        finally:
            self._running = False
            # 固定レート: 前回開始からの経過時間を差し引いてスケジュールする
            # （stop() 済みの場合は再スケジュールしない。停止フラグをロック下で確認）
            elapsed = time.perf_counter() - start_time
            delay = max(PREGENERATION_INTERVAL_SECONDS - elapsed, _MIN_RESCHEDULE_DELAY_SECONDS)
            with self._lock:
                self._schedule_next_locked(delay=delay)

    def _resolve_time_end(self) -> datetime.datetime:
        """事前生成期間の終端を決定する（分単位に正規化）。

        フロントエンドは終了日時を data-range API の latest にクランプするため、
        DB の最新データ時刻を優先する。fetch_data_range はモジュールレベルで
        キャッシュされており、data-range API と同じ値が得られる。
        取得失敗時・データが未来時刻の場合は現在時刻を使う。
        """
        now = my_lib.time.now().replace(second=0, microsecond=0)

        config = self._config
        if config is None:
            return now

        try:
            conn = amdar.database.postgresql.open(
                config.database.host,
                config.database.port,
                config.database.name,
                config.database.user,
                config.database.password,
                apply_schema=False,
            )
            with contextlib.closing(conn):
                result = amdar.database.postgresql.fetch_data_range(conn)
        except Exception:
            logging.warning("[PREGEN] Failed to fetch latest data time, using current time", exc_info=True)
            return now

        latest = result.latest
        if latest is None:
            return now
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=my_lib.time.get_zoneinfo())

        return min(now, latest.replace(second=0, microsecond=0))

    def _generate_graphs(
        self,
        time_start: datetime.datetime,
        time_end: datetime.datetime,
        limit_altitude: bool,
    ) -> int:
        """対象グラフを生成し、成功件数を返す。"""
        generated = 0
        for graph_name in _PREGENERATION_GRAPHS:
            if self._stop_requested:
                break
            try:
                # 残り TTL が次回事前生成までの間隔を割るキャッシュはヒット扱いせず、
                # 実際に再生成・保存させる。これを指定しないと、期限切れ間近の
                # 自分自身のキャッシュに許容差ヒットして空振り（再配信のみ）となり、
                # キャッシュを新鮮に保てない。
                graph_service.generate_sync(
                    graph_name,
                    time_start,
                    time_end,
                    limit_altitude,
                    min_cache_ttl_remaining=PREGENERATION_INTERVAL_SECONDS,
                )
                generated += 1
            except Exception:
                logging.exception("[PREGEN] Failed to generate %s", graph_name)

        return generated


# モジュールレベルの共有インスタンス
cache_pregenerator = CachePregenerator()
