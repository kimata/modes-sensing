"""地理座標計算ユーティリティ

距離計算や座標変換を行う共通関数を提供します。
"""

import math

import amdar.constants


def simple_distance(lat: float, lon: float, ref_lat: float, ref_lon: float) -> float:
    """簡易距離計算（高速、低精度）

    緯度経度差からの概算。Haversine 公式より高速だが精度は劣る。
    近距離（数百km以内）では実用的な精度。

    Args:
        lat: 対象点の緯度 [度]
        lon: 対象点の経度 [度]
        ref_lat: 基準点の緯度 [度]
        ref_lon: 基準点の経度 [度]

    Returns:
        距離 [km]
    """
    lat_dist = (lat - ref_lat) * amdar.constants.KM_PER_DEGREE_LATITUDE
    lon_dist = (lon - ref_lon) * amdar.constants.KM_PER_DEGREE_LATITUDE * math.cos(math.radians(ref_lat))
    return math.sqrt(lat_dist**2 + lon_dist**2)


def haversine_distance(ref_lat: float, ref_lon: float, lat: float, lon: float) -> float:
    """Haversine 公式による精密距離計算

    地球を球体と仮定した正確な距離計算。
    計算コストは simple_distance より高いが、長距離でも正確。

    Args:
        ref_lat: 基準点の緯度 [度]
        ref_lon: 基準点の経度 [度]
        lat: 目標点の緯度 [度]
        lon: 目標点の経度 [度]

    Returns:
        距離 [km]
    """
    R = 6371.0  # 地球の半径 [km]
    lat1 = math.radians(ref_lat)
    lat2 = math.radians(lat)
    dlat = math.radians(lat - ref_lat)
    dlon = math.radians(lon - ref_lon)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c
