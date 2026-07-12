"""航空物理計算ユーティリティ

Mode S の BDS 5,0/6,0 メッセージから気温・風向・風速を算出する共通関数を提供します。
リアルタイム受信（modes/receiver.py）とファイル解析（sources/aggregator.py）の
両経路から使用されます。
"""

from __future__ import annotations

import math

import amdar.core.geo
from amdar.core.types import WindData

# 空気の物性定数
SPECIFIC_HEAT_RATIO_AIR: float = 1.403
"""比熱比（空気）"""

MOLAR_MASS_AIR: float = 28.966e-3
"""分子量（空気） [kg/mol]"""

GAS_CONSTANT: float = 8.314472
"""気体定数 [J/(mol·K)]"""


def calc_temperature(trueair_ms: float, mach: float) -> float:
    """真気速度とマッハ数から気温を計算する

    音速 = TAS / Mach、音速^2 = γRT/M の関係から気温を求めます。

    Args:
        trueair_ms: 真気速度 [m/s]
        mach: マッハ数

    Returns:
        気温 [℃]

    Raises:
        ValueError: mach が 0 以下の場合
    """
    if mach <= 0:
        msg = f"mach must be positive: {mach}"
        raise ValueError(msg)

    K = MOLAR_MASS_AIR / SPECIFIC_HEAT_RATIO_AIR / GAS_CONSTANT
    sound_speed = trueair_ms / mach

    return sound_speed * sound_speed * K - 273.15


def calc_wind(
    latitude: float,
    longitude: float,
    trackangle: float,
    groundspeed_ms: float,
    heading: float,
    trueair_ms: float,
) -> WindData:
    """対地速度ベクトルと対気速度ベクトルの差から風向・風速を計算する

    機首方位（磁方位）は磁気偏角で真方位に補正します。

    Args:
        latitude: 緯度 [度]（磁気偏角計算用）
        longitude: 経度 [度]（磁気偏角計算用）
        trackangle: 対地進行方向 [度]（真北基準）
        groundspeed_ms: 対地速度 [m/s]
        heading: 機首方位 [度]（磁北基準）
        trueair_ms: 真気速度 [m/s]

    Returns:
        WindData（x, y, angle, speed）
    """
    magnetic_declination = amdar.core.geo.calc_magnetic_declination(latitude, longitude)

    ground_dir = math.pi / 2 - math.radians(trackangle)
    ground_x = groundspeed_ms * math.cos(ground_dir)
    ground_y = groundspeed_ms * math.sin(ground_dir)

    air_dir = math.pi / 2 - math.radians(heading) + math.radians(magnetic_declination)
    air_x = trueair_ms * math.cos(air_dir)
    air_y = trueair_ms * math.sin(air_dir)

    wind_x = ground_x - air_x
    wind_y = ground_y - air_y

    return WindData(
        x=wind_x,
        y=wind_y,
        # NOTE: 北を 0 として，風が来る方の角度
        angle=math.degrees(
            (math.pi / 2 - math.atan2(wind_y, wind_x) + 2 * math.pi + math.pi) % (2 * math.pi)
        ),
        speed=math.sqrt(wind_x * wind_x + wind_y * wind_y),
    )
