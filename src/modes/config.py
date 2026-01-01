"""アプリケーション設定の dataclass 定義"""

from __future__ import annotations

import pathlib
from dataclasses import dataclass, field
from typing import Any

import my_lib.notify.slack


@dataclass(frozen=True)
class DecoderConfig:
    """Mode S デコーダ設定"""

    host: str
    port: int


@dataclass(frozen=True)
class ModesConfig:
    """Mode S 設定"""

    decoder: DecoderConfig


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
class WebappConfig:
    """Webアプリケーション設定"""

    static_dir_path: str


@dataclass(frozen=True)
class LivenessFileConfig:
    """Liveness ファイル設定"""

    collector: pathlib.Path
    receiver: pathlib.Path


@dataclass(frozen=True)
class LivenessConfig:
    """Liveness 設定"""

    file: LivenessFileConfig


@dataclass(frozen=True)
class Config:
    """アプリケーション全体の設定"""

    modes: ModesConfig
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
    parsed = my_lib.notify.slack.parse_config(slack_dict)

    # SlackErrorOnlyConfig または SlackEmptyConfig のみを許可
    if isinstance(parsed, (my_lib.notify.slack.SlackErrorOnlyConfig, my_lib.notify.slack.SlackEmptyConfig)):
        return parsed

    # その他の設定タイプの場合、SlackErrorOnlyConfig に変換を試みる
    # NOTE: hasattr チェック後でも型が絞り込まれないため getattr を使用 (B009 を無視)
    if hasattr(parsed, "error") and hasattr(parsed, "bot_token") and hasattr(parsed, "from_name"):
        return my_lib.notify.slack.SlackErrorOnlyConfig(
            bot_token=getattr(parsed, "bot_token"),  # noqa: B009
            from_name=getattr(parsed, "from_name"),  # noqa: B009
            error=getattr(parsed, "error"),  # noqa: B009
        )

    # 変換できない場合は空設定を返す
    return my_lib.notify.slack.SlackEmptyConfig()


def load_from_dict(config_dict: dict[str, Any], base_dir: pathlib.Path) -> Config:
    """辞書形式の設定を Config に変換する"""
    return Config(
        modes=ModesConfig(
            decoder=DecoderConfig(
                host=config_dict["modes"]["decoder"]["host"],
                port=config_dict["modes"]["decoder"]["port"],
            ),
        ),
        database=DatabaseConfig(
            host=config_dict["database"]["host"],
            port=config_dict["database"]["port"],
            name=config_dict["database"]["name"],
            user=config_dict["database"]["user"],
            password=config_dict["database"]["pass"],
        ),
        filter=FilterConfig(
            area=Area(
                lat=CoordinateRef(ref=config_dict["filter"]["area"]["lat"]["ref"]),
                lon=CoordinateRef(ref=config_dict["filter"]["area"]["lon"]["ref"]),
                distance=config_dict["filter"]["area"]["distance"],
            ),
        ),
        font=FontConfig(
            path=pathlib.Path(config_dict["font"]["path"]),
            map=config_dict["font"]["map"],
        ),
        webapp=WebappConfig(
            static_dir_path=config_dict["webapp"]["static_dir_path"],
        ),
        liveness=LivenessConfig(
            file=LivenessFileConfig(
                collector=pathlib.Path(config_dict["liveness"]["file"]["collector"]),
                receiver=pathlib.Path(config_dict["liveness"]["file"]["receiver"]),
            ),
        ),
        slack=_parse_slack_config(config_dict.get("slack", {})),
        base_dir=base_dir,
    )
