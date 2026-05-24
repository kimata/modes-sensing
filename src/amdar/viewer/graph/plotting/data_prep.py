"""グラフ描画用のデータ前処理。"""

from __future__ import annotations

from dataclasses import dataclass, field

import matplotlib.dates
import numpy
import pandas

import amdar.database.postgresql
from amdar.constants import (
    GRAPH_ALT_MAX,
    GRAPH_ALT_MIN,
    GRAPH_TEMPERATURE_THRESHOLD,
)


@dataclass
class PreparedData:
    """描画用に整形されたデータ。

    DataFrame は風向グラフでのみ使用するため遅延作成する。
    """

    count: int
    times: numpy.ndarray
    time_numeric: numpy.ndarray
    altitudes: numpy.ndarray
    temperatures: numpy.ndarray
    wind_x: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    wind_y: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    wind_speed: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    wind_angle: numpy.ndarray = field(default_factory=lambda: numpy.array([], dtype=numpy.float32))
    _dataframe: pandas.DataFrame | None = field(default=None, repr=False)

    @property
    def dataframe(self) -> pandas.DataFrame:
        """風向グラフ用 DataFrame を遅延作成する。"""
        if self._dataframe is not None:
            return self._dataframe

        if self.count == 0:
            self._dataframe = pandas.DataFrame()
            return self._dataframe

        df_data: dict[str, numpy.ndarray] = {
            "time": self.times,
            "time_numeric": self.time_numeric,
            "altitude": self.altitudes,
            "temperature": self.temperatures,
        }
        if len(self.wind_x) > 0:
            df_data["wind_x"] = self.wind_x
            df_data["wind_y"] = self.wind_y
            df_data["wind_speed"] = self.wind_speed
            df_data["wind_angle"] = self.wind_angle

        self._dataframe = pandas.DataFrame(df_data)
        return self._dataframe


@dataclass
class WindFilteredData:
    """風データのフィルタリング結果。"""

    altitudes: numpy.ndarray
    wind_x: numpy.ndarray
    wind_y: numpy.ndarray
    time_numeric: numpy.ndarray


@dataclass
class GridData:
    """補間グリッドデータ（等高線・ヒートマップ用）。"""

    time_mesh: numpy.ndarray
    alt_mesh: numpy.ndarray
    temp_grid: numpy.ndarray
    time_min: float
    time_max: float
    alt_min: float
    alt_max: float


def _empty_prepared_data() -> PreparedData:
    empty = numpy.array([], dtype=numpy.float32)
    return PreparedData(
        count=0,
        times=numpy.array([], dtype="datetime64[us]"),
        time_numeric=empty,
        altitudes=empty,
        temperatures=empty,
    )


def prepare_data(raw_data) -> PreparedData:
    """dict のリストから PreparedData を生成する（ローカルテスト用）。

    本番では :func:`prepare_data_numpy` を使う。
    """
    if not raw_data:
        return _empty_prepared_data()

    data_length = len(raw_data)
    temperatures = numpy.empty(data_length, dtype=numpy.float64)
    altitudes = numpy.empty(data_length, dtype=numpy.float64)

    for i, record in enumerate(raw_data):
        temperatures[i] = record["temperature"]
        altitudes[i] = record["altitude"]

    valid_mask = (
        (temperatures > GRAPH_TEMPERATURE_THRESHOLD)
        & (numpy.isfinite(temperatures))
        & (numpy.isfinite(altitudes))
        & (altitudes >= GRAPH_ALT_MIN)
        & (altitudes <= GRAPH_ALT_MAX)
    )

    if not valid_mask.any():
        return _empty_prepared_data()

    valid_indices = numpy.where(valid_mask)[0]
    valid_count = len(valid_indices)

    clean_temperatures = numpy.ascontiguousarray(temperatures[valid_mask], dtype=numpy.float32)
    clean_altitudes = numpy.ascontiguousarray(altitudes[valid_mask], dtype=numpy.float32)

    times_list = [raw_data[i]["time"] for i in valid_indices]
    times = pandas.to_datetime(times_list, utc=False, cache=True).to_numpy()
    time_numeric = numpy.ascontiguousarray(matplotlib.dates.date2num(times))

    filtered_records = [raw_data[i] for i in valid_indices] if valid_count < data_length else raw_data
    clean_df = pandas.DataFrame(filtered_records) if filtered_records else pandas.DataFrame()

    result = PreparedData(
        count=valid_count,
        times=times,
        time_numeric=time_numeric,
        altitudes=clean_altitudes,
        temperatures=clean_temperatures,
    )
    result._dataframe = clean_df
    return result


def prepare_data_numpy(numpy_data: amdar.database.postgresql.NumpyFetchResult) -> PreparedData:
    """NumPy 配列形式のデータから描画用データを準備する（高速版）。"""
    if numpy_data.count == 0:
        return _empty_prepared_data()

    times = numpy_data.time
    altitudes = numpy_data.altitude
    temperatures = numpy_data.temperature

    valid_mask = (
        (temperatures > GRAPH_TEMPERATURE_THRESHOLD)
        & numpy.isfinite(temperatures)
        & numpy.isfinite(altitudes)
        & (altitudes >= GRAPH_ALT_MIN)
        & (altitudes <= GRAPH_ALT_MAX)
    )

    valid_count = int(numpy.count_nonzero(valid_mask))
    if valid_count == 0:
        return _empty_prepared_data()

    clean_times = times[valid_mask]
    clean_altitudes = numpy.ascontiguousarray(altitudes[valid_mask], dtype=numpy.float32)
    clean_temperatures = numpy.ascontiguousarray(temperatures[valid_mask], dtype=numpy.float32)

    # datetime64[us] から matplotlib date number (1970-01-01=0) に変換
    # 注: time_numeric は日付計算の精度確保のため float64 を維持
    time_numeric = clean_times.astype("float64") / (86400 * 1e6)
    time_numeric = numpy.ascontiguousarray(time_numeric)

    empty = numpy.array([], dtype=numpy.float32)
    if (
        numpy_data.wind_x is not None
        and numpy_data.wind_y is not None
        and numpy_data.wind_speed is not None
        and numpy_data.wind_angle is not None
    ):
        wind_x = numpy.ascontiguousarray(numpy_data.wind_x[valid_mask], dtype=numpy.float32)
        wind_y = numpy.ascontiguousarray(numpy_data.wind_y[valid_mask], dtype=numpy.float32)
        wind_speed = numpy.ascontiguousarray(numpy_data.wind_speed[valid_mask], dtype=numpy.float32)
        wind_angle = numpy.ascontiguousarray(numpy_data.wind_angle[valid_mask], dtype=numpy.float32)
    else:
        wind_x = wind_y = wind_speed = wind_angle = empty

    return PreparedData(
        count=valid_count,
        times=clean_times,
        time_numeric=time_numeric,
        altitudes=clean_altitudes,
        temperatures=clean_temperatures,
        wind_x=wind_x,
        wind_y=wind_y,
        wind_speed=wind_speed,
        wind_angle=wind_angle,
    )
