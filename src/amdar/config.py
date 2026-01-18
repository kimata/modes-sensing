"""アプリケーション設定の dataclass 定義"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any

import my_lib.config
import my_lib.notify.slack
import my_lib.safe_access
import my_lib.webapp.config


@dataclass(frozen=True)
class HostPortConfig:
    """ホストとポートの設定"""

    host: str
    port: int


@dataclass(frozen=True)
class DecoderConfig:
    """デコーダ設定（modes は必須、vdl2 はオプション）"""

    modes: HostPortConfig
    vdl2: HostPortConfig | None = None


@dataclass(frozen=True)
class DatabaseConfig:
    """データベース接続設定"""

    host: str
    port: int
    name: str
    user: str
    password: str  # YAML では 'pass' だが、予約語を避ける


@dataclass(frozen=True)
class CoordinateRef:
    """緯度・経度の基準値"""

    ref: float


@dataclass(frozen=True)
class Area:
    """エリアフィルタ設定"""

    lat: CoordinateRef
    lon: CoordinateRef
    distance: int


@dataclass(frozen=True)
class FilterConfig:
    """フィルタ設定"""

    area: Area


@dataclass(frozen=True)
class FontConfig:
    """フォント設定"""

    path: pathlib.Path
    map: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class WebappConfig(my_lib.webapp.config.WebappConfig):
    """Webアプリケーション設定（my_lib.webapp.config.WebappConfig を拡張）"""

    cache_dir_path: pathlib.Path = field(default_factory=lambda: pathlib.Path("cache"))


@dataclass(frozen=True)
class LivenessReceiverFileConfig:
    """レシーバー別 liveness ファイル設定"""

    modes: pathlib.Path
    vdl2: pathlib.Path | None = None


@dataclass(frozen=True)
class LivenessFileConfig:
    """Liveness ファイル設定"""

    collector: pathlib.Path
    receiver: LivenessReceiverFileConfig


@dataclass(frozen=True)
class LivenessScheduleConfig:
    """時間帯別タイムアウト設定"""

    daytime_start_hour: int = 7
    daytime_end_hour: int = 22
    daytime_timeout_sec: int = 60
    nighttime_timeout_sec: int = 3600


@dataclass(frozen=True)
class LivenessConfig:
    """Liveness 設定"""

    file: LivenessFileConfig
    schedule: LivenessScheduleConfig


@dataclass(frozen=True)
class Config:
    """アプリケーション全体の設定"""

    decoder: DecoderConfig
    database: DatabaseConfig
    filter: FilterConfig
    font: FontConfig
    webapp: WebappConfig
    liveness: LivenessConfig
    slack: my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig = field(
        default_factory=my_lib.notify.slack.SlackEmptyConfig
    )
    base_dir: pathlib.Path = field(default_factory=pathlib.Path.cwd)


def _parse_slack_config(
    slack_dict: dict[str, Any],
) -> my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig:
    """Slack 設定をパースして SlackErrorOnlyConfig または SlackEmptyConfig を返す"""
    parsed = my_lib.notify.slack.SlackConfig.parse(slack_dict)

    # SlackErrorOnlyConfig または SlackEmptyConfig のみを許可
    if isinstance(parsed, my_lib.notify.slack.SlackErrorOnlyConfig | my_lib.notify.slack.SlackEmptyConfig):
        return parsed

    # その他の設定タイプの場合、SlackErrorOnlyConfig に変換を試みる
    # SafeAccess を使用して属性を安全に取得
    parsed_safe: Any = my_lib.safe_access.safe(parsed)  # type: ignore[attr-defined]
    bot_token = parsed_safe.bot_token.value()
    from_name = parsed_safe.from_name.value()
    error = parsed_safe.error.value()
    if bot_token is not None and from_name is not None and error is not None:
        return my_lib.notify.slack.SlackErrorOnlyConfig(
            bot_token=bot_token,
            from_name=from_name,
            error=error,
        )

    # 変換できない場合は空設定を返す
    return my_lib.notify.slack.SlackEmptyConfig()


def _resolve_path(base_dir: pathlib.Path, path_str: str) -> pathlib.Path:
    """相対パスを base_dir 基準で解決する

    Args:
        base_dir: 基準ディレクトリ
        path_str: パス文字列

    Returns:
        解決されたパス（絶対パスの場合はそのまま）
    """
    path = pathlib.Path(path_str)
    if path.is_absolute():
        return path
    return base_dir / path


def load_config(config_file: str) -> Config:
    """設定ファイルを読み込んで Config を返す

    スキーマ検証を行い、base_dir を現在の作業ディレクトリに設定します。

    Args:
        config_file: 設定ファイルのパス

    Returns:
        Config インスタンス
    """
    import amdar.constants

    config_dict = my_lib.config.load(config_file, amdar.constants.get_schema_path())
    return load_from_dict(config_dict, pathlib.Path.cwd())


def load_from_dict(config_dict: dict[str, Any], base_dir: pathlib.Path) -> Config:
    """辞書形式の設定を Config に変換する"""
    cfg: Any = my_lib.config.accessor(config_dict)  # type: ignore[attr-defined]

    # VDL2 設定（オプション）
    vdl2_config = None
    vdl2_host = cfg.get("decoder", "vdl2", "host")
    if vdl2_host is not None:
        vdl2_config = HostPortConfig(
            host=vdl2_host,
            port=cfg.get("decoder", "vdl2", "port"),
        )

    # VDL2 liveness ファイル（オプション）
    vdl2_liveness_path = cfg.get("liveness", "file", "receiver", "vdl2")
    vdl2_liveness = _resolve_path(base_dir, vdl2_liveness_path) if vdl2_liveness_path else None

    return Config(
        decoder=DecoderConfig(
            modes=HostPortConfig(
                host=cfg.get("decoder", "modes", "host"),
                port=cfg.get("decoder", "modes", "port"),
            ),
            vdl2=vdl2_config,
        ),
        database=DatabaseConfig(
            host=cfg.get("database", "host"),
            port=cfg.get("database", "port"),
            name=cfg.get("database", "name"),
            user=cfg.get("database", "user"),
            password=cfg.get("database", "pass"),
        ),
        filter=FilterConfig(
            area=Area(
                lat=CoordinateRef(ref=cfg.get("filter", "area", "lat", "ref")),
                lon=CoordinateRef(ref=cfg.get("filter", "area", "lon", "ref")),
                distance=cfg.get("filter", "area", "distance"),
            ),
        ),
        font=FontConfig(
            path=_resolve_path(base_dir, cfg.get_str("font", "path")),
            map=cfg.get_dict("font", "map"),
        ),
        webapp=WebappConfig(
            static_dir_path=_resolve_path(base_dir, cfg.get_str("webapp", "static_dir_path")),
            cache_dir_path=_resolve_path(base_dir, cfg.get_str("webapp", "cache_dir_path")),
        ),
        liveness=LivenessConfig(
            file=LivenessFileConfig(
                collector=_resolve_path(base_dir, cfg.get_str("liveness", "file", "collector")),
                receiver=LivenessReceiverFileConfig(
                    modes=_resolve_path(base_dir, cfg.get_str("liveness", "file", "receiver", "modes")),
                    vdl2=vdl2_liveness,
                ),
            ),
            schedule=LivenessScheduleConfig(
                daytime_start_hour=cfg.get("liveness", "schedule", "daytime", "start_hour"),
                daytime_end_hour=cfg.get("liveness", "schedule", "daytime", "end_hour"),
                daytime_timeout_sec=cfg.get("liveness", "schedule", "daytime", "timeout_sec"),
                nighttime_timeout_sec=cfg.get("liveness", "schedule", "nighttime", "timeout_sec"),
            ),
        ),
        slack=_parse_slack_config(cfg.get_dict("slack")),
        base_dir=base_dir,
    )
