"""VDL2 (dumpvdl2) から気象データを受信するモジュール

ZMQ SUB ソケットを使用して dumpvdl2 から JSON メッセージを受信し、
ACARS メッセージから気象データを抽出します。

XID メッセージの高度情報と ACARS メッセージの気象情報を
同一航空機から短時間内に受信した場合、フラグメント結合を行います。

IntegratedBuffer を使用する場合、ADS-B からの高度補完も行います。
"""

from __future__ import annotations

import logging
import pathlib
import queue
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import my_lib.footprint
import zmq

import amdar.sources.vdl2.parser

if TYPE_CHECKING:
    import amdar.database.postgresql
    from amdar.sources.aggregator import IntegratedBuffer

_should_terminate = threading.Event()
_liveness_file: pathlib.Path | None = None

# フラグメントの有効期限（秒）
_FRAGMENT_TIMEOUT = 300  # 5分

# フラグメントバッファ（航空機ごと）
_fragment_buffer: dict[str, _AircraftFragment] = {}
_fragment_lock = threading.Lock()


@dataclass
class _AircraftFragment:
    """航空機ごとのフラグメントデータ"""

    icao: str
    xid_data: amdar.sources.vdl2.parser.XidLocationData | None = None
    xid_timestamp: float = 0.0
    acars_data: amdar.sources.vdl2.parser.AcarsWeatherData | None = None
    acars_timestamp: float = 0.0


def _try_combine_fragments(
    icao: str,
    ref_lat: float,
    ref_lon: float,
    received_at: datetime,
) -> amdar.database.postgresql.MeasurementData | None:
    """フラグメントを結合して MeasurementData を生成する

    同一航空機から XID（高度）と ACARS（気象）の両方が
    短時間内に受信されている場合、結合してデータを生成する。

    Args:
        icao: 航空機アドレス
        ref_lat: 基準点の緯度
        ref_lon: 基準点の経度

    Returns:
        結合された MeasurementData、または結合できない場合は None
    """
    with _fragment_lock:
        if icao not in _fragment_buffer:
            return None

        fragment = _fragment_buffer[icao]
        xid = fragment.xid_data
        acars = fragment.acars_data

        # 両方のデータが必要
        if xid is None or acars is None:
            return None

        # 時間差をチェック
        time_diff = abs(fragment.xid_timestamp - fragment.acars_timestamp)
        if time_diff > _FRAGMENT_TIMEOUT:
            return None

        # ACARS に高度がある場合はそのまま使用
        if acars.altitude_ft is not None:
            return amdar.sources.vdl2.parser.convert_to_measurement_data(
                acars, ref_lat, ref_lon, received_at=received_at
            )

        # ACARS に温度がない場合は結合不可
        if acars.temperature_c is None:
            return None

        # XID の高度を使用して新しい AcarsWeatherData を作成
        combined_acars = amdar.sources.vdl2.parser.AcarsWeatherData(
            flight=acars.flight,
            reg=acars.reg,
            timestamp=acars.timestamp,
            latitude=xid.latitude if xid.latitude is not None else acars.latitude,
            longitude=xid.longitude if xid.longitude is not None else acars.longitude,
            altitude_ft=xid.altitude_ft,
            temperature_c=acars.temperature_c,
            wind_dir_deg=acars.wind_dir_deg,
            wind_speed_kt=acars.wind_speed_kt,
        )

        # フラグメントをクリア（結合済み）
        _fragment_buffer[icao] = _AircraftFragment(icao=icao)

        logging.debug(
            "VDL2 fragment combined: %s, XID alt=%d, ACARS temp=%.1f",
            icao,
            xid.altitude_ft,
            acars.temperature_c,
        )

        return amdar.sources.vdl2.parser.convert_to_measurement_data(
            combined_acars, ref_lat, ref_lon, received_at=received_at
        )


def _cleanup_old_fragments() -> None:
    """古いフラグメントを削除する"""
    current_time = time.time()
    with _fragment_lock:
        expired_keys = []
        for icao, fragment in _fragment_buffer.items():
            # 最後の更新から一定時間経過したフラグメントを削除
            latest_time = max(fragment.xid_timestamp, fragment.acars_timestamp)
            if current_time - latest_time > _FRAGMENT_TIMEOUT * 2:
                expired_keys.append(icao)

        for key in expired_keys:
            del _fragment_buffer[key]


def _try_altitude_補完_from_buffer(
    acars_data: amdar.sources.vdl2.parser.AcarsWeatherData,
    icao: str | None,
    buffer: IntegratedBuffer,
    ref_lat: float,
    ref_lon: float,
    received_at: datetime,
) -> amdar.database.postgresql.MeasurementData | None:
    """IntegratedBuffer を使って高度補完を試みる

    Args:
        acars_data: ACARS 気象データ
        icao: ICAO アドレス
        buffer: ADS-B データを保持するバッファ
        ref_lat: 基準点の緯度
        ref_lon: 基準点の経度

    Returns:
        補完された MeasurementData、または補完失敗時は None
    """
    # 気温がない場合は処理不要
    if acars_data.temperature_c is None:
        return None

    # 補完対象のタイムスタンプ
    timestamp = acars_data.timestamp or datetime.now(UTC)

    # ICAO またはコールサインで検索
    identifier = icao or acars_data.flight
    if not identifier:
        return None

    # バッファから高度・位置を取得
    result = buffer.get_altitude_at(identifier, timestamp)
    if result is None:
        return None

    altitude_m, interp_lat, interp_lon, altitude_source = result

    # 位置を補完
    final_lat = acars_data.latitude if acars_data.latitude is not None else interp_lat
    final_lon = acars_data.longitude if acars_data.longitude is not None else interp_lon

    # 高度を ft に変換
    altitude_ft = int(altitude_m / 0.3048)

    # 新しい AcarsWeatherData を作成
    combined_acars = amdar.sources.vdl2.parser.AcarsWeatherData(
        flight=acars_data.flight,
        reg=acars_data.reg,
        timestamp=acars_data.timestamp,
        latitude=final_lat,
        longitude=final_lon,
        altitude_ft=altitude_ft,
        temperature_c=acars_data.temperature_c,
        wind_dir_deg=acars_data.wind_dir_deg,
        wind_speed_kt=acars_data.wind_speed_kt,
    )

    logging.debug(
        "VDL2 altitude補完 (buffer): %s, alt=%.0fft (source=%s)",
        identifier,
        altitude_ft,
        altitude_source,
    )

    return amdar.sources.vdl2.parser.convert_to_measurement_data(
        combined_acars, ref_lat, ref_lon, received_at=received_at
    )


def _worker(
    host: str,
    port: int,
    data_queue: queue.Queue[amdar.database.postgresql.MeasurementData],
    ref_lat: float,
    ref_lon: float,
    buffer: IntegratedBuffer | None = None,
) -> None:
    """ZMQ 受信ワーカー

    dumpvdl2 から JSON メッセージを受信し、ACARS 気象データを抽出して
    キューに追加します。XID と ACARS のフラグメント結合も行います。

    buffer が指定されている場合、ADS-B からの高度補完も試みます。

    Args:
        host: dumpvdl2 のホスト
        port: dumpvdl2 の ZMQ ポート
        data_queue: 抽出したデータを追加するキュー
        ref_lat: 基準点の緯度
        ref_lon: 基準点の経度
        buffer: ADS-B データを保持するバッファ（オプション）
    """
    ctx = zmq.Context()
    socket = ctx.socket(zmq.SUB)
    socket.connect(f"tcp://{host}:{port}")
    socket.setsockopt(zmq.SUBSCRIBE, b"")
    socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5秒タイムアウト

    logging.info("VDL2 receiver started: %s:%d (buffer=%s)", host, port, buffer is not None)

    total_count = 0
    weather_count = 0
    combined_count = 0
    buffer_combined_count = 0
    cleanup_counter = 0

    while not _should_terminate.is_set():
        try:
            msg = socket.recv()
            total_count += 1
            current_time = time.time()

            # ICAO アドレスを取得
            icao = amdar.sources.vdl2.parser.get_icao_from_message(msg)

            # XID 位置データを処理
            xid_data = amdar.sources.vdl2.parser.parse_xid_location(msg)
            if xid_data and icao:
                with _fragment_lock:
                    if icao not in _fragment_buffer:
                        _fragment_buffer[icao] = _AircraftFragment(icao=icao)
                    _fragment_buffer[icao].xid_data = xid_data
                    _fragment_buffer[icao].xid_timestamp = current_time
                logging.debug("VDL2 XID received: %s, alt=%d", icao, xid_data.altitude_ft)

            # ACARS 気象データを処理
            acars_data = amdar.sources.vdl2.parser.parse_acars_weather(msg)
            if acars_data:
                # 受信時刻を取得（VDL2 データ内のタイムスタンプは無視）
                received_at = datetime.now(UTC)

                # まず直接変換を試みる
                measurement = amdar.sources.vdl2.parser.convert_to_measurement_data(
                    acars_data, ref_lat, ref_lon, received_at=received_at
                )
                if measurement:
                    weather_count += 1
                    data_queue.put(measurement)
                    # Liveness ファイルを更新
                    if _liveness_file is not None:
                        my_lib.footprint.update(_liveness_file)
                    logging.debug(
                        "VDL2 weather data: %s alt=%d temp=%.1f",
                        measurement.callsign,
                        measurement.altitude,
                        measurement.temperature,
                    )
                else:
                    # 高度がない場合、まず IntegratedBuffer から補完を試みる
                    if buffer is not None:
                        buffer_補完 = _try_altitude_補完_from_buffer(
                            acars_data, icao, buffer, ref_lat, ref_lon, received_at
                        )
                        if buffer_補完:
                            buffer_combined_count += 1
                            weather_count += 1
                            data_queue.put(buffer_補完)
                            # Liveness ファイルを更新
                            if _liveness_file is not None:
                                my_lib.footprint.update(_liveness_file)
                            logging.info(
                                "VDL2 buffer補完 weather: %s alt=%d temp=%.1f",
                                buffer_補完.callsign,
                                buffer_補完.altitude,
                                buffer_補完.temperature,
                            )
                            continue

                    # IntegratedBuffer で補完できなかった場合、内部フラグメントバッファに保存
                    if icao:
                        with _fragment_lock:
                            if icao not in _fragment_buffer:
                                _fragment_buffer[icao] = _AircraftFragment(icao=icao)
                            _fragment_buffer[icao].acars_data = acars_data
                            _fragment_buffer[icao].acars_timestamp = current_time

                        # フラグメント結合を試みる
                        combined = _try_combine_fragments(icao, ref_lat, ref_lon, received_at)
                        if combined:
                            combined_count += 1
                            weather_count += 1
                            data_queue.put(combined)
                            # Liveness ファイルを更新
                            if _liveness_file is not None:
                                my_lib.footprint.update(_liveness_file)
                            logging.info(
                                "VDL2 combined weather: %s alt=%d temp=%.1f",
                                combined.callsign,
                                combined.altitude,
                                combined.temperature,
                            )

            # 定期的にフラグメントをクリーンアップ
            cleanup_counter += 1
            if cleanup_counter >= 100:
                _cleanup_old_fragments()
                cleanup_counter = 0

            # 進捗ログ（1000メッセージごと）
            if total_count % 1000 == 0:
                logging.info(
                    "VDL2 received %d messages, %d weather data (%d combined, %d buffer補完)",
                    total_count,
                    weather_count,
                    combined_count,
                    buffer_combined_count,
                )

        except zmq.Again:
            # タイムアウト、ループを継続
            continue
        except Exception:
            logging.exception("VDL2 receive error")

    socket.close()
    ctx.term()
    logging.info(
        "VDL2 receiver stopped: %d messages, %d weather data (%d combined, %d buffer補完)",
        total_count,
        weather_count,
        combined_count,
        buffer_combined_count,
    )


def start(
    host: str,
    port: int,
    data_queue: queue.Queue[amdar.database.postgresql.MeasurementData],
    ref_lat: float,
    ref_lon: float,
    buffer: IntegratedBuffer | None = None,
    liveness_file: pathlib.Path | None = None,
) -> threading.Thread:
    """VDL2 受信ワーカーを開始する

    Args:
        host: dumpvdl2 のホスト
        port: dumpvdl2 の ZMQ ポート
        data_queue: 抽出したデータを追加するキュー
        ref_lat: 基準点の緯度
        ref_lon: 基準点の経度
        buffer: ADS-B データを保持するバッファ（高度補完用、オプション）
        liveness_file: Liveness ファイルパス（オプション）

    Returns:
        開始したワーカースレッド
    """
    global _liveness_file
    _should_terminate.clear()
    _liveness_file = liveness_file

    # フラグメントバッファをクリア
    with _fragment_lock:
        _fragment_buffer.clear()

    thread = threading.Thread(
        target=_worker,
        args=(host, port, data_queue, ref_lat, ref_lon, buffer),
        daemon=True,
    )
    thread.start()
    return thread


def term() -> None:
    """受信を終了する"""
    _should_terminate.set()
