#!/usr/bin/env python3
"""ADS-B と VDL2 のデータ統合・高度補完モジュール

このモジュールは以下の機能を提供します：
- ADS-B の位置・高度情報のバッファリング
- VDL2 メッセージへの高度情報補完
- コールサイン ↔ ICAO アドレスのマッピング管理
- ファイルからのデータ解析と統合

Usage:
  aggregator.py [-m MODES_FILE] [-v VDL2_FILE] [--lat LAT] [--lon LON] [--no-filter] [-D]

Options:
  -m MODES_FILE     : Mode-S ダンプファイル（*...; 形式）
  -v VDL2_FILE      : VDL2 ダンプファイル（JSON Lines 形式）
  --lat LAT         : 基準緯度 [default: 35.682677]
  --lon LON         : 基準経度 [default: 139.762230]
  --no-filter       : 外れ値除去を無効にする
  -D                : デバッグモードで動作します．
"""

from __future__ import annotations

import logging
import math
import pathlib
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from queue import Queue
from typing import TYPE_CHECKING

import pyModeS

import amdar.sources.outlier
from amdar.core.types import WeatherObservation, WindData

if TYPE_CHECKING:
    pass


@dataclass
class AltitudeEntry:
    """高度履歴エントリ

    ADS-B から取得した航空機の高度・位置情報を保持します。
    """

    timestamp: datetime
    """データ取得時刻"""

    altitude_m: float
    """高度 [m]"""

    latitude: float | None = None
    """緯度 [度]"""

    longitude: float | None = None
    """経度 [度]"""

    message_index: int = 0
    """メッセージの順序インデックス（ファイル解析時用）"""


@dataclass
class IntegratedBuffer:
    """ADS-B と VDL2 のデータを統合管理するバッファ

    VDL2 メッセージ単体では高度情報が欠落することが多いため、
    前後1分以内の ADS-B データで補完します。

    設計方針:
    - バッファの最大サイズは設けない（時間ウィンドウで自動破棄）
    - 補完は時刻的に近いものを優先、同一時刻の場合は後のデータを使用
    - 高度がない場合はレコードを破棄
    """

    window_seconds: float = 60.0
    """補完ウィンドウ（秒）"""

    _altitude_by_icao: dict[str, deque[AltitudeEntry]] = field(default_factory=dict)
    """ICAO -> 高度履歴のマッピング"""

    _callsign_to_icao: dict[str, str] = field(default_factory=dict)
    """コールサイン -> ICAO のマッピング"""

    _current_time: datetime | None = field(default=None)
    """現在の基準時刻（最新データの時刻）"""

    _message_counter: int = field(default=0)
    """メッセージカウンタ（ファイル解析時の順序管理用）"""

    def update_time(self, timestamp: datetime) -> None:
        """基準時刻を更新し、古いデータを破棄

        ファイル解析時も新データの時刻で判断するため、
        この関数で基準時刻を更新します。

        Args:
            timestamp: 新しい基準時刻
        """
        self._current_time = timestamp
        # ウィンドウの2倍より古いデータを破棄
        cutoff = timestamp - timedelta(seconds=self.window_seconds * 2)
        self._cleanup_before(cutoff)

    def _cleanup_before(self, cutoff: datetime) -> None:
        """指定時刻より古いエントリを削除

        Args:
            cutoff: この時刻より古いエントリを削除
        """
        empty_icaos = []
        for icao, entries in self._altitude_by_icao.items():
            # 古いエントリを先頭から削除
            while entries and entries[0].timestamp < cutoff:
                entries.popleft()
            if not entries:
                empty_icaos.append(icao)

        # 空になった ICAO エントリを削除
        for icao in empty_icaos:
            del self._altitude_by_icao[icao]

    def add_adsb_position(
        self,
        icao: str,
        callsign: str | None,
        timestamp: datetime,
        altitude_m: float,
        lat: float | None = None,
        lon: float | None = None,
    ) -> None:
        """ADS-B の位置・高度情報を追加

        Args:
            icao: ICAO アドレス（24bit hex）
            callsign: コールサイン（便名）
            timestamp: データ取得時刻
            altitude_m: 高度 [m]
            lat: 緯度 [度]
            lon: 経度 [度]
        """
        if not icao:
            return

        icao = icao.upper()
        self._message_counter += 1

        # ICAO -> 高度履歴に追加
        if icao not in self._altitude_by_icao:
            self._altitude_by_icao[icao] = deque()

        entry = AltitudeEntry(
            timestamp=timestamp,
            altitude_m=altitude_m,
            latitude=lat,
            longitude=lon,
            message_index=self._message_counter,
        )
        self._altitude_by_icao[icao].append(entry)

        # コールサイン -> ICAO マッピングを更新
        if callsign:
            callsign = callsign.strip().upper()
            if callsign:
                self._callsign_to_icao[callsign] = icao

    def get_altitude_at(
        self,
        icao_or_callsign: str,
        timestamp: datetime,
    ) -> tuple[float, float | None, float | None, str] | None:
        """指定時刻に最も近い高度・位置を取得

        優先順位:
        1. 時刻差が最小のエントリ
        2. 時刻差が同じ場合は後のエントリ（より新しい情報）

        Args:
            icao_or_callsign: ICAO アドレスまたはコールサイン
            timestamp: 対象時刻

        Returns:
            (altitude_m, lat, lon, source) または None
            source: "adsb" | "interpolated"
        """
        # ICAO を解決
        icao = self._resolve_identifier(icao_or_callsign)
        if not icao:
            return None

        entries = self._altitude_by_icao.get(icao)
        if not entries:
            return None

        # ウィンドウ内のエントリをフィルタ
        window_entries = [
            e for e in entries if abs((e.timestamp - timestamp).total_seconds()) <= self.window_seconds
        ]

        if not window_entries:
            return None

        # 時刻差でソート、同一差の場合は後のエントリを優先
        best = min(
            window_entries,
            key=lambda e: (
                abs((e.timestamp - timestamp).total_seconds()),
                -e.timestamp.timestamp(),
            ),
        )

        # 完全一致なら "adsb"、それ以外は "interpolated"
        time_diff = abs((best.timestamp - timestamp).total_seconds())
        source = "adsb" if time_diff < 1.0 else "interpolated"

        return (best.altitude_m, best.latitude, best.longitude, source)

    def get_altitude_by_order(
        self,
        icao_or_callsign: str,
        message_index: int,
        max_distance: int = 1000,
    ) -> tuple[float, float | None, float | None, str] | None:
        """メッセージ順序ベースで高度を取得（時刻なしファイル用）

        時刻情報がないファイル解析時に使用します。

        Args:
            icao_or_callsign: ICAO アドレスまたはコールサイン
            message_index: 対象メッセージのインデックス
            max_distance: 前後何メッセージまで探索するか

        Returns:
            (altitude_m, lat, lon, source) または None
        """
        icao = self._resolve_identifier(icao_or_callsign)
        if not icao:
            return None

        entries = self._altitude_by_icao.get(icao)
        if not entries:
            return None

        # インデックス差でフィルタ
        nearby_entries = [e for e in entries if abs(e.message_index - message_index) <= max_distance]

        if not nearby_entries:
            return None

        # インデックス差が最小のエントリを選択
        best = min(
            nearby_entries,
            key=lambda e: (abs(e.message_index - message_index), -e.message_index),
        )

        return (best.altitude_m, best.latitude, best.longitude, "interpolated")

    def resolve_icao(self, callsign: str) -> str | None:
        """コールサインから ICAO を解決

        Args:
            callsign: コールサイン

        Returns:
            ICAO アドレスまたは None
        """
        if not callsign:
            return None
        return self._callsign_to_icao.get(callsign.strip().upper())

    def _resolve_identifier(self, icao_or_callsign: str) -> str | None:
        """ICAO またはコールサインから ICAO を解決

        Args:
            icao_or_callsign: ICAO アドレスまたはコールサイン

        Returns:
            ICAO アドレスまたは None
        """
        if not icao_or_callsign:
            return None

        identifier = icao_or_callsign.strip().upper()

        # まず ICAO として直接検索
        if identifier in self._altitude_by_icao:
            return identifier

        # コールサインとして検索
        return self._callsign_to_icao.get(identifier)

    def get_stats(self) -> dict[str, int]:
        """バッファの統計情報を取得

        Returns:
            統計情報の辞書
        """
        total_entries = sum(len(entries) for entries in self._altitude_by_icao.values())
        return {
            "aircraft_count": len(self._altitude_by_icao),
            "total_entries": total_entries,
            "callsign_mappings": len(self._callsign_to_icao),
            "message_counter": self._message_counter,
        }

    def clear(self) -> None:
        """バッファをクリア"""
        self._altitude_by_icao.clear()
        self._callsign_to_icao.clear()
        self._current_time = None
        self._message_counter = 0


class RealtimeAggregator:
    """リアルタイム受信用のアグリゲーター

    ADS-B と VDL2 の両データソースからリアルタイムでデータを受信し、
    統合された WeatherObservation を生成します。
    外れ値検出機能を内蔵し、両データソースに対して統一的に適用します。
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        ref_lat: float = 35.682677,
        ref_lon: float = 139.762230,
        enable_outlier_filter: bool = True,
    ):
        """初期化

        Args:
            window_seconds: 補完ウィンドウ（秒）
            ref_lat: 基準点緯度
            ref_lon: 基準点経度
            enable_outlier_filter: 外れ値検出を有効にするか
        """
        self._buffer = IntegratedBuffer(window_seconds=window_seconds)
        self._ref_lat = ref_lat
        self._ref_lon = ref_lon
        self._output_queue: Queue[WeatherObservation] = Queue()
        self._enable_outlier_filter = enable_outlier_filter
        self._outlier_detector: amdar.sources.outlier.OutlierDetector | None = (
            amdar.sources.outlier.OutlierDetector() if enable_outlier_filter else None
        )

    @property
    def buffer(self) -> IntegratedBuffer:
        """内部バッファへのアクセス"""
        return self._buffer

    @property
    def output_queue(self) -> Queue[WeatherObservation]:
        """出力キュー"""
        return self._output_queue

    @property
    def outlier_detector(self) -> amdar.sources.outlier.OutlierDetector | None:
        """外れ値検出器へのアクセス"""
        return self._outlier_detector

    def init_outlier_history(self, data: list[tuple[float, float]]) -> None:
        """外れ値検出用の履歴データを初期化

        Args:
            data: (altitude, temperature) のタプルのリスト
        """
        if self._outlier_detector is None:
            return
        for altitude, temperature in data:
            self._outlier_detector.add_history(altitude, temperature)
        logging.info("RealtimeAggregator: 外れ値検出用履歴データを初期化しました: %d件", len(data))

    def _check_outlier(
        self,
        altitude: float,
        temperature: float | None,
        callsign: str | None,
        source: str,
    ) -> bool:
        """外れ値かどうかを判定

        Args:
            altitude: 高度 [m]
            temperature: 気温 [°C]
            callsign: コールサイン
            source: データソース（ログ用）

        Returns:
            外れ値の場合 True、正常値または検出無効の場合 False
        """
        if self._outlier_detector is None:
            return False
        if temperature is None:
            return False

        is_outlier = self._outlier_detector.is_outlier(altitude, temperature, callsign or "")
        if is_outlier:
            logging.warning(
                "%s 外れ値検出: callsign=%s, altitude=%.1fm, temperature=%.1f°C",
                source,
                callsign or "Unknown",
                altitude,
                temperature,
            )
        return is_outlier

    def _add_to_outlier_history(self, altitude: float, temperature: float | None) -> None:
        """正常値を外れ値検出用履歴に追加

        Args:
            altitude: 高度 [m]
            temperature: 気温 [°C]
        """
        if self._outlier_detector is None:
            return
        if temperature is None:
            return
        self._outlier_detector.add_history(altitude, temperature)

    def process_modes_position(
        self,
        icao: str,
        callsign: str | None,
        timestamp: datetime,
        altitude_m: float,
        lat: float | None = None,
        lon: float | None = None,
    ) -> None:
        """Mode-S の位置・高度情報を処理

        バッファに追加し、後続の VDL2 補完に使用できるようにします。

        Args:
            icao: ICAO アドレス
            callsign: コールサイン
            timestamp: データ取得時刻
            altitude_m: 高度 [m]
            lat: 緯度
            lon: 経度
        """
        self._buffer.update_time(timestamp)
        self._buffer.add_adsb_position(
            icao=icao,
            callsign=callsign,
            timestamp=timestamp,
            altitude_m=altitude_m,
            lat=lat,
            lon=lon,
        )

    def process_modes_weather(
        self,
        *,
        icao: str,
        callsign: str | None,
        timestamp: datetime,
        altitude_m: float,
        lat: float | None = None,
        lon: float | None = None,
        temperature_c: float | None = None,
        wind: WindData | None = None,
        data_source: str = "bds50_60",
    ) -> WeatherObservation | None:
        """Mode-S の気象データを処理

        バッファ更新と WeatherObservation の生成を行います。

        Args:
            icao: ICAO アドレス
            callsign: コールサイン
            timestamp: データ取得時刻
            altitude_m: 高度 [m]
            lat: 緯度
            lon: 経度
            temperature_c: 気温 [℃]
            wind: 風データ
            data_source: データソース種別

        Returns:
            生成された WeatherObservation または None
        """
        self._buffer.update_time(timestamp)
        self._buffer.add_adsb_position(
            icao=icao,
            callsign=callsign,
            timestamp=timestamp,
            altitude_m=altitude_m,
            lat=lat,
            lon=lon,
        )

        # 気象データがなければ None
        if temperature_c is None and wind is None:
            return None

        # 距離を計算
        distance = 0.0
        if lat is not None and lon is not None:
            distance = self._calculate_distance(lat, lon)

        observation = WeatherObservation(
            timestamp=timestamp,
            icao=icao,
            callsign=callsign,
            altitude=altitude_m,
            latitude=lat,
            longitude=lon,
            distance=distance,
            temperature=temperature_c,
            wind=wind,
            method="mode-s",
            data_source=data_source,
            altitude_source="adsb",
        )

        if not observation.is_valid():
            return None

        # 外れ値検出
        if self._check_outlier(altitude_m, temperature_c, callsign, "Mode-S"):
            return None

        # 正常値を出力キューに追加し、履歴にも追加
        self._output_queue.put(observation)
        self._add_to_outlier_history(altitude_m, temperature_c)
        return observation

    def process_vdl2_weather(
        self,
        *,
        icao: str | None,
        callsign: str | None,
        timestamp: datetime,
        altitude_m: float | None = None,
        lat: float | None = None,
        lon: float | None = None,
        temperature_c: float | None = None,
        wind: WindData | None = None,
        data_source: str = "acars",
    ) -> WeatherObservation | None:
        """VDL2 の気象データを処理

        高度がない場合は ADS-B バッファから補完を試みます。

        Args:
            icao: ICAO アドレス（XID から取得、ない場合あり）
            callsign: コールサイン
            timestamp: データ取得時刻
            altitude_m: 高度 [m]（ない場合あり）
            lat: 緯度
            lon: 経度
            temperature_c: 気温 [℃]
            wind: 風データ
            data_source: データソース種別

        Returns:
            生成された WeatherObservation または None（高度補完失敗時）
        """
        self._buffer.update_time(timestamp)

        # 気象データがなければ処理不要
        if temperature_c is None and wind is None:
            return None

        altitude_source = "acars"
        final_altitude = altitude_m
        final_lat = lat
        final_lon = lon

        # 高度がない場合、ADS-B から補完を試みる
        if final_altitude is None or final_altitude <= 0:
            identifier = icao or callsign
            if identifier:
                result = self._buffer.get_altitude_at(identifier, timestamp)
                if result:
                    final_altitude, interp_lat, interp_lon, altitude_source = result
                    # 位置も補完（VDL2 に位置がない場合）
                    if final_lat is None:
                        final_lat = interp_lat
                    if final_lon is None:
                        final_lon = interp_lon

        # 高度がなければ破棄
        if final_altitude is None or final_altitude <= 0:
            logging.debug(
                "VDL2 weather data discarded: no altitude available for %s/%s",
                icao,
                callsign,
            )
            return None

        # 距離を計算
        distance = 0.0
        if final_lat is not None and final_lon is not None:
            distance = self._calculate_distance(final_lat, final_lon)

        observation = WeatherObservation(
            timestamp=timestamp,
            icao=icao,
            callsign=callsign,
            altitude=final_altitude,
            latitude=final_lat,
            longitude=final_lon,
            distance=distance,
            temperature=temperature_c,
            wind=wind,
            method="vdl2",
            data_source=data_source,
            altitude_source=altitude_source,
        )

        if not observation.is_valid():
            return None

        # 外れ値検出
        if self._check_outlier(final_altitude, temperature_c, callsign, "VDL2"):
            return None

        # 正常値を出力キューに追加し、履歴にも追加
        self._output_queue.put(observation)
        self._add_to_outlier_history(final_altitude, temperature_c)
        return observation

    def _calculate_distance(self, lat: float, lon: float) -> float:
        """基準点からの距離を計算

        簡易的な計算（球面近似）を使用します。

        Args:
            lat: 緯度
            lon: 経度

        Returns:
            距離 [km]
        """
        import math

        # 地球の半径 [km]
        R = 6371.0

        lat1 = math.radians(self._ref_lat)
        lat2 = math.radians(lat)
        dlat = math.radians(lat - self._ref_lat)
        dlon = math.radians(lon - self._ref_lon)

        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def get_stats(self) -> dict[str, int]:
        """統計情報を取得"""
        stats = self._buffer.get_stats()
        stats["output_queue_size"] = self._output_queue.qsize()
        if self._outlier_detector is not None:
            stats["outlier_history_count"] = self._outlier_detector.history_count
        return stats

    def clear(self) -> None:
        """内部状態をクリア"""
        self._buffer.clear()
        if self._outlier_detector is not None:
            self._outlier_detector.clear_history()
        # キューはクリアしない（消費者が処理する）


class FileAggregator:
    """ファイル解析用のアグリゲーター

    Mode-S と VDL2 のファイルを解析し、統合された WeatherObservation を生成します。
    Mode-S ファイルには時刻情報がないため、メッセージ順序ベースの補完を行います。
    """

    def __init__(
        self,
        ref_lat: float = 35.682677,
        ref_lon: float = 139.762230,
        max_index_distance: int = 1000,
    ):
        """初期化

        Args:
            ref_lat: 基準点緯度
            ref_lon: 基準点経度
            max_index_distance: 順序ベース補完の最大インデックス距離
        """
        self._buffer = IntegratedBuffer()
        self._ref_lat = ref_lat
        self._ref_lon = ref_lon
        self._max_index_distance = max_index_distance
        self._results: list[WeatherObservation] = []
        self._message_index = 0

    def _next_index(self) -> int:
        """次のメッセージインデックスを取得"""
        self._message_index += 1
        return self._message_index

    def _calculate_distance(self, lat: float, lon: float) -> float:
        """基準点からの距離を計算 [km]"""
        R = 6371.0
        lat1 = math.radians(self._ref_lat)
        lat2 = math.radians(lat)
        dlat = math.radians(lat - self._ref_lat)
        dlon = math.radians(lon - self._ref_lon)
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def _calc_temperature(self, trueair_ms: float, mach: float) -> float:
        """真気速度とマッハ数から気温を計算"""
        if mach <= 0:
            return -999.0
        # 音速 = TAS / Mach
        # 音速^2 = γ * R * T → T = 音速^2 / (γ * R)
        # γ = 1.4, R = 287 J/(kg·K)
        sound_speed = trueair_ms / mach
        temperature_k = sound_speed**2 / (1.4 * 287)
        return temperature_k - 273.15

    def _calc_magnetic_declination(self, lat: float, lon: float) -> float:
        """磁気偏差を概算（日本周辺で使用）"""
        delta_latitude = lat - 37.0
        delta_longitude = lon - 138.0
        return (
            -7.6
            + (0.009 / 60) * delta_latitude
            - (0.082 / 60) * delta_longitude
            + (0.107 / 60) * delta_latitude * delta_longitude
            - (0.655 / 60) * delta_longitude * delta_longitude
        )

    def _calc_wind(
        self,
        lat: float,
        lon: float,
        trackangle: float,
        groundspeed_ms: float,
        heading: float,
        trueair_ms: float,
    ) -> WindData:
        """風向・風速を計算"""
        mag_dec = self._calc_magnetic_declination(lat, lon)

        ground_dir = math.pi / 2 - math.radians(trackangle)
        ground_x = groundspeed_ms * math.cos(ground_dir)
        ground_y = groundspeed_ms * math.sin(ground_dir)

        air_dir = math.pi / 2 - math.radians(heading) + math.radians(mag_dec)
        air_x = trueair_ms * math.cos(air_dir)
        air_y = trueair_ms * math.sin(air_dir)

        wind_x = ground_x - air_x
        wind_y = ground_y - air_y

        return WindData(
            x=wind_x,
            y=wind_y,
            angle=math.degrees(
                (math.pi / 2 - math.atan2(wind_y, wind_x) + 2 * math.pi + math.pi) % (2 * math.pi)
            ),
            speed=math.sqrt(wind_x * wind_x + wind_y * wind_y),
        )

    def parse_modes_file(self, file_path: pathlib.Path) -> list[WeatherObservation]:
        """Mode-S ファイルを解析

        ファイルを読み込み、ADS-B 位置情報をバッファに登録しながら
        気象データを WeatherObservation として抽出します。

        Args:
            file_path: Mode-S メッセージファイル（*...; 形式）

        Returns:
            抽出された WeatherObservation のリスト
        """
        # ICAO ごとのフラグメント管理
        fragments: dict[str, dict] = {}
        results: list[WeatherObservation] = []

        # ダミー時刻（ファイル解析時は時刻がないため）
        base_time = datetime.now(UTC)

        with file_path.open() as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith("*"):
                    continue

                msg = line[1:].rstrip(";")
                if len(msg) < 22:
                    continue

                msg_index = self._next_index()
                # ダミー時刻を進める（1メッセージ=10ms）
                dummy_time = base_time + timedelta(milliseconds=msg_index * 10)

                try:
                    icao = str(pyModeS.icao(msg))
                    dformat = pyModeS.df(msg)

                    if icao not in fragments:
                        fragments[icao] = {
                            "callsign": None,
                            "altitude_ft": None,
                            "lat": None,
                            "lon": None,
                            "bds50": None,
                            "bds60": None,
                            "bds44": None,
                        }
                    frag = fragments[icao]

                    # DF=17,18: ADS-B
                    if dformat in (17, 18) and len(msg) == 28:
                        code = pyModeS.typecode(msg)
                        if code is None:
                            continue

                        # 位置情報（高度含む）
                        if (5 <= code <= 18) or (20 <= code <= 22):
                            altitude = pyModeS.adsb.altitude(msg)
                            if altitude and altitude > 0:
                                frag["altitude_ft"] = float(altitude)
                                altitude_m = frag["altitude_ft"] * 0.3048
                                try:
                                    lat, lon = pyModeS.adsb.position_with_ref(
                                        msg, self._ref_lat, self._ref_lon
                                    )
                                    if lat is not None and lon is not None:
                                        frag["lat"] = lat
                                        frag["lon"] = lon
                                except Exception:
                                    # 位置計算の失敗は無視（高度情報のみでも有用）
                                    logging.debug("Failed to calculate position for %s", icao)

                                # バッファに登録（VDL2 補完用）
                                self._buffer.add_adsb_position(
                                    icao=icao,
                                    callsign=frag["callsign"],
                                    timestamp=dummy_time,
                                    altitude_m=altitude_m,
                                    lat=frag["lat"],
                                    lon=frag["lon"],
                                )

                        # コールサイン
                        elif 1 <= code <= 4:
                            callsign = pyModeS.adsb.callsign(msg).rstrip("_")
                            if callsign:
                                frag["callsign"] = callsign
                                # コールサイン更新時もバッファ更新
                                if frag["altitude_ft"]:
                                    self._buffer.add_adsb_position(
                                        icao=icao,
                                        callsign=callsign,
                                        timestamp=dummy_time,
                                        altitude_m=frag["altitude_ft"] * 0.3048,
                                        lat=frag["lat"],
                                        lon=frag["lon"],
                                    )

                    # DF=20,21: Comm-B
                    elif dformat in (20, 21) and len(msg) == 28:
                        # BDS 4,4 を優先
                        if pyModeS.bds.bds44.is44(msg):
                            temperature = pyModeS.bds.bds44.temp44(msg)
                            wind_data = pyModeS.bds.bds44.wind44(msg)
                            if temperature is not None and wind_data is not None:
                                wind_speed, wind_direction = wind_data
                                if (
                                    wind_speed is not None
                                    and wind_direction is not None
                                    and frag["altitude_ft"] is not None
                                ):
                                    obs = WeatherObservation.from_imperial(
                                        icao=icao,
                                        callsign=frag["callsign"],
                                        altitude_ft=frag["altitude_ft"],
                                        latitude=frag["lat"],
                                        longitude=frag["lon"],
                                        temperature_c=temperature,
                                        wind_speed_kt=wind_speed,
                                        wind_direction_deg=wind_direction,
                                        distance=self._calculate_distance(
                                            frag["lat"] or self._ref_lat,
                                            frag["lon"] or self._ref_lon,
                                        ),
                                        method="mode-s",
                                        data_source="bds44",
                                        altitude_source="adsb",
                                    )
                                    if obs.is_valid():
                                        results.append(obs)
                                    frag["bds44"] = None
                            continue

                        # BDS 5,0
                        if pyModeS.bds.bds50.is50(msg):
                            trackangle = pyModeS.commb.trk50(msg)
                            groundspeed = pyModeS.commb.gs50(msg)
                            trueair = pyModeS.commb.tas50(msg)
                            if all(v is not None for v in (trackangle, groundspeed, trueair)):
                                frag["bds50"] = (trackangle, groundspeed, trueair)

                        # BDS 6,0
                        elif pyModeS.bds.bds60.is60(msg):
                            heading = pyModeS.commb.hdg60(msg)
                            indicatedair = pyModeS.commb.ias60(msg)
                            mach = pyModeS.commb.mach60(msg)
                            if all(v is not None for v in (heading, indicatedair, mach)):
                                frag["bds60"] = (heading, indicatedair, mach)

                        # BDS 5,0 + 6,0 ペアリング
                        if (
                            frag["bds50"] is not None
                            and frag["bds60"] is not None
                            and frag["altitude_ft"] is not None
                            and frag["lat"] is not None
                            and frag["lon"] is not None
                        ):
                            trackangle, groundspeed, trueair = frag["bds50"]
                            heading, indicatedair, mach = frag["bds60"]

                            trueair_ms = float(trueair) * 0.514  # type: ignore[arg-type]
                            groundspeed_ms = float(groundspeed) * 0.514  # type: ignore[arg-type]
                            mach_f = float(mach)  # type: ignore[arg-type]

                            temperature_c = self._calc_temperature(trueair_ms, mach_f)
                            if temperature_c >= -100:
                                wind = self._calc_wind(
                                    frag["lat"],
                                    frag["lon"],
                                    float(trackangle),  # type: ignore[arg-type]
                                    groundspeed_ms,
                                    float(heading),  # type: ignore[arg-type]
                                    trueair_ms,
                                )

                                obs = WeatherObservation(
                                    icao=icao,
                                    callsign=frag["callsign"],
                                    altitude=frag["altitude_ft"] * 0.3048,
                                    latitude=frag["lat"],
                                    longitude=frag["lon"],
                                    temperature=temperature_c,
                                    wind=wind,
                                    distance=self._calculate_distance(frag["lat"], frag["lon"]),
                                    method="mode-s",
                                    data_source="bds50_60",
                                    altitude_source="adsb",
                                )
                                if obs.is_valid():
                                    results.append(obs)

                            frag["bds50"] = None
                            frag["bds60"] = None

                except Exception:
                    logging.debug("Mode-S message parse failed: %s", msg)

        return results

    def parse_vdl2_file(self, file_path: pathlib.Path) -> list[WeatherObservation]:
        """VDL2 ファイルを解析

        ファイルを読み込み、高度がない場合は ADS-B バッファから補完します。
        タイムスタンプは受信時刻（ファイル解析時は解析時の時刻）を使用し、
        VDL2 データ内のタイムスタンプは無視します。

        Args:
            file_path: VDL2 メッセージファイル（JSON Lines 形式）

        Returns:
            抽出された WeatherObservation のリスト
        """
        import amdar.sources.vdl2.parser as vdl2_parser

        results: list[WeatherObservation] = []

        # ダミー時刻（ファイル解析時は解析時刻を基準に使用）
        base_time = datetime.now(UTC)

        with file_path.open("rb") as f:
            for line in f:
                msg_index = self._next_index()
                # ダミー時刻を進める（1メッセージ=10ms）
                dummy_time = base_time + timedelta(milliseconds=msg_index * 10)

                # ACARS 気象データを解析
                acars = vdl2_parser.parse_acars_weather(line)
                if acars is None:
                    # XID から位置情報を取得してバッファに登録
                    xid = vdl2_parser.parse_xid_location(line)
                    if xid and xid.altitude_ft:
                        self._buffer.add_adsb_position(
                            icao=xid.icao,
                            callsign=None,
                            timestamp=dummy_time,
                            altitude_m=xid.altitude_ft * 0.3048,
                            lat=xid.latitude,
                            lon=xid.longitude,
                        )
                    continue

                # 気温がなければスキップ
                if acars.temperature_c is None:
                    continue

                # 高度の補完を試みる
                altitude_ft = acars.altitude_ft
                altitude_source = "acars"
                final_lat = acars.latitude
                final_lon = acars.longitude

                if altitude_ft is None or altitude_ft <= 0:
                    # ICAO またはコールサインで補完
                    icao = vdl2_parser.get_icao_from_message(line)
                    identifier = icao or acars.flight

                    if identifier:
                        # 順序ベースで高度を取得
                        result = self._buffer.get_altitude_by_order(
                            identifier, msg_index, self._max_index_distance
                        )

                        if result:
                            altitude_m, interp_lat, interp_lon, altitude_source = result
                            altitude_ft = int(altitude_m / 0.3048)
                            if final_lat is None:
                                final_lat = interp_lat
                            if final_lon is None:
                                final_lon = interp_lon

                # 高度がなければ破棄
                if altitude_ft is None or altitude_ft <= 0:
                    continue

                # 距離計算
                distance = 0.0
                if final_lat is not None and final_lon is not None:
                    distance = self._calculate_distance(final_lat, final_lon)

                # WeatherObservation を生成（タイムスタンプは受信時刻を使用）
                obs = WeatherObservation.from_imperial(
                    timestamp=dummy_time,
                    icao=vdl2_parser.get_icao_from_message(line),
                    callsign=acars.flight,
                    altitude_ft=altitude_ft,
                    latitude=final_lat,
                    longitude=final_lon,
                    temperature_c=float(acars.temperature_c),
                    wind_speed_kt=(float(acars.wind_speed_kt) if acars.wind_speed_kt is not None else None),
                    wind_direction_deg=(
                        float(acars.wind_dir_deg) if acars.wind_dir_deg is not None else None
                    ),
                    distance=distance,
                    method="vdl2",
                    data_source="acars",
                    altitude_source=altitude_source,
                )

                if obs.is_valid():
                    results.append(obs)

        return results

    def get_results(self) -> list[WeatherObservation]:
        """蓄積された結果を取得"""
        return self._results

    def get_stats(self) -> dict[str, int]:
        """統計情報を取得"""
        return {
            **self._buffer.get_stats(),
            "results_count": len(self._results),
        }


def parse_from_files(
    modes_file: pathlib.Path | None = None,
    vdl2_file: pathlib.Path | None = None,
    ref_lat: float = 35.682677,
    ref_lon: float = 139.762230,
    filter_outliers: bool = True,
) -> list[WeatherObservation]:
    """ファイルからデータを解析

    両ファイルを処理し、VDL2 の高度補完を ADS-B データで行います。
    Mode-S ファイルを先に処理してバッファを構築し、
    その後 VDL2 ファイルを処理して高度補完を行います。

    Args:
        modes_file: Mode-S メッセージファイル（*...; 形式）
        vdl2_file: VDL2 メッセージファイル（JSON Lines 形式）
        ref_lat: 基準点緯度
        ref_lon: 基準点経度
        filter_outliers: 外れ値除去を適用するか（デフォルト: True）

    Returns:
        時刻順にソートされた WeatherObservation のリスト
        （時刻がない場合は元の順序を維持）
    """
    if modes_file is None and vdl2_file is None:
        return []

    aggregator = FileAggregator(ref_lat=ref_lat, ref_lon=ref_lon)
    results: list[WeatherObservation] = []

    # Mode-S ファイルを先に処理（バッファ構築のため）
    if modes_file is not None and modes_file.exists():
        modes_results = aggregator.parse_modes_file(modes_file)
        results.extend(modes_results)
        logging.info(
            "Mode-S file parsed: %d weather observations, %s",
            len(modes_results),
            aggregator.get_stats(),
        )

    # VDL2 ファイルを処理（高度補完を適用）
    if vdl2_file is not None and vdl2_file.exists():
        vdl2_results = aggregator.parse_vdl2_file(vdl2_file)
        results.extend(vdl2_results)
        logging.info(
            "VDL2 file parsed: %d weather observations, %s",
            len(vdl2_results),
            aggregator.get_stats(),
        )

    # 時刻でソート（時刻がない場合は後ろに配置）
    def sort_key(obs: WeatherObservation) -> tuple[int, float]:
        if obs.timestamp is not None:
            return (0, obs.timestamp.timestamp())
        return (1, 0.0)

    results.sort(key=sort_key)

    # 外れ値除去
    if filter_outliers:
        detector = amdar.sources.outlier.OutlierDetector()
        results = detector.filter_observations(results)

    return results


if __name__ == "__main__":
    import docopt
    import my_lib.logger
    import my_lib.pretty

    assert __doc__ is not None  # noqa: S101
    args = docopt.docopt(__doc__)

    modes_file = args["-m"]
    vdl2_file = args["-v"]
    ref_lat = float(args["--lat"])
    ref_lon = float(args["--lon"])
    no_filter = args["--no-filter"]
    debug_mode = args["-D"]

    my_lib.logger.init("aggregator", level=logging.DEBUG if debug_mode else logging.INFO)

    if modes_file is None and vdl2_file is None:
        logging.error("Mode-S ファイル (-m) または VDL2 ファイル (-v) を指定してください")
        raise SystemExit(1)

    modes_path = pathlib.Path(modes_file) if modes_file else None
    vdl2_path = pathlib.Path(vdl2_file) if vdl2_file else None

    logging.info("解析開始")
    logging.info("  Mode-S ファイル: %s", modes_path or "(なし)")
    logging.info("  VDL2 ファイル: %s", vdl2_path or "(なし)")
    logging.info("  基準座標: (%.6f, %.6f)", ref_lat, ref_lon)
    logging.info("  外れ値除去: %s", "無効" if no_filter else "有効")

    observations = parse_from_files(
        modes_file=modes_path,
        vdl2_file=vdl2_path,
        ref_lat=ref_lat,
        ref_lon=ref_lon,
        filter_outliers=not no_filter,
    )

    logging.info("解析完了: %d 件の気象データを取得", len(observations))

    for obs in observations:
        logging.info(my_lib.pretty.format(obs))
