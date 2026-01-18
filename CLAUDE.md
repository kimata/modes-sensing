# CLAUDE.md - プロジェクトガイドライン

このファイルは Claude Code がこのリポジトリで作業する際のガイドラインです。

## プロジェクト概要

**modes-sensing** は、航空機から送信される Mode S 信号を受信・解析し、気象データ（気温・風向・風速）を抽出して可視化する Python + React システムです。

### 主な機能

- Mode S メッセージ (BDS 4,4/4,5) のリアルタイム受信・デコード
- 気温・風向・風速の計算（真気速度・マッハ数・地速・機首方位から）
- 機械学習による外れ値検出（sklearn IsolationForest）
- PostgreSQL/SQLite へのデータ保存
- 8種類のグラフ生成（散布図、ヒートマップ、等高線、温度、風向など）
- 非同期グラフ生成（マルチプロセス + ジョブキュー）
- React フロントエンドによるインタラクティブな可視化
- Slack 連携によるエラー通知

## ディレクトリ構成

```
src/
├── collect_vdl2.py             # VDL2 のみ収集（デバッグ用）
└── amdar/
    ├── __main__.py             # amdar コマンド
    ├── cli/
    │   ├── collect.py          # amdar エントリポイント実装 (Mode S + VDL2 統合収集)
    │   ├── webui.py            # amdar-webui (Flask Web サーバー)
    │   └── healthz.py          # amdar-healthz (ヘルスチェック + Slack 通知)
    ├── config.py               # 設定管理（dataclass ベース）
    ├── sources/
    │   ├── modes/receiver.py   # Mode S 受信・デコード
    │   └── vdl2/receiver.py    # VDL2 受信・デコード
    ├── database/
    │   ├── postgresql.py       # PostgreSQL データアクセス
    │   └── sqlite.py           # SQLite データアクセス（開発用）
    └── viewer/api/
        ├── graph.py            # グラフ生成 API
        └── job_manager.py      # 非同期ジョブ管理

frontend/                          # React フロントエンド
├── src/
│   ├── App.tsx                 # メインアプリケーション
│   ├── components/             # UI コンポーネント
│   ├── hooks/                  # カスタムフック
│   └── types/                  # 型定義
├── package.json
└── vite.config.ts

tests/
├── conftest.py                 # pytest 共通フィクスチャ
├── unit/                       # ユニットテスト
├── integration/                # 統合テスト
└── e2e/                        # E2E テスト（Playwright）

config.example.yaml             # 設定ファイルサンプル
config.schema                   # 設定の JSON Schema
```

## 開発コマンド

### 依存関係のインストール

```bash
uv sync
```

### アプリケーション実行

```bash
# データ収集
uv run amdar                              # 通常実行
uv run amdar -c config.yaml -n 100        # 設定指定、100件で停止
uv run amdar -D                           # デバッグモード

# Web サーバー
uv run amdar-webui                        # 通常実行（ポート 5000）
uv run amdar-webui -p 8080                # ポート指定
uv run amdar-webui -D                     # デバッグモード
```

### テスト実行

```bash
uv run pytest                             # テスト実行（並列、E2E 除外）
uv run pytest tests/unit/                 # ユニットテストのみ
uv run pytest tests/integration/          # 統合テストのみ
uv run pytest tests/e2e/                  # E2E テスト（外部サーバー必要）
```

### 型チェック

```bash
uv run mypy src/                          # mypy による型チェック
uv run pyright                            # pyright による型チェック
```

### リント・フォーマット

```bash
uv run ruff check src/                    # リントチェック
uv run ruff format src/                   # フォーマット
```

### React ビルド

```bash
cd react
npm install
npm run build                             # 本番ビルド
npm run dev                               # 開発サーバー
```

## コーディング規約

### Python バージョン

- Python 3.11 以上（推奨: 3.13）

### スタイル

- 最大行長: 110 文字（ruff 設定）
- ruff lint ルール: E, F, W, I, B, UP
- dataclass を積極的に使用（frozen dataclass 推奨）
- 型ヒントを必ず記述

### 型チェック

- mypy と pyright の両方でチェック

## アーキテクチャ

### データ収集フロー

```
Mode S 信号 (1090MHz)
    ↓
dump1090-fa (外部デコーダ)
    ↓ JSON (port 30002)
receiver.py
├── メッセージ解析
├── 気温計算 (calc_temperature)
├── 風向・風速計算 (calc_wind)
├── 外れ値検出 (sklearn)
└── MeasurementData 生成
    ↓ Queue
database_postgresql.py
└── PostgreSQL INSERT
```

### Web サーバーフロー

```
クライアント (React)
    ↓ POST /api/graph/job
webui.py (Flask)
    ↓
graph.py
├── JobManager.create_job()
├── ProcessPool で非同期生成
│   ├── データ取得 (fetch_by_time_numpy)
│   ├── データ準備 (prepare_data_numpy)
│   └── グラフ生成 (plot_*)
└── PNG 画像返却
```

### 主要クラス

- **Config** (`config.py`): 設定を保持する frozen dataclass
- **PreparedData** (`graph.py`): グラフ生成用に準備されたデータ
- **JobManager** (`job_manager.py`): 非同期ジョブ管理（シングルトン）
- **ProcessPoolManager** (`graph.py`): マルチプロセスプール管理

### データベース集約戦略

長期間のデータを効率的に処理するため、期間に応じて集約レベルを変更：

| 期間     | 集約       | 用途         |
| -------- | ---------- | ------------ |
| 14日以内 | 生データ   | 高精度分析   |
| 14-90日  | 30分×250m  | 中期分析     |
| 90日以上 | 3時間×250m | 長期トレンド |

### グラフタイプ

| グラフ       | graph_name     | 説明              |
| ------------ | -------------- | ----------------- |
| 2D散布図     | scatter_2d     | 時間-高度-温度    |
| 3D散布図     | scatter_3d     | 立体データ分布    |
| ヒートマップ | heatmap        | 連続温度変化      |
| 2D等高線     | contour_2d     | 等温線            |
| 3D等高線     | contour_3d     | 3次元等温面       |
| 密度プロット | density        | 高度-温度分布密度 |
| 温度プロット | temperature    | 時間-温度推移     |
| 風向プロット | wind_direction | 高度別風向・風速  |

## 重要な注意事項

### プロジェクト設定ファイルの編集禁止

`pyproject.toml` をはじめとする一般的なプロジェクト管理ファイルは、`../py-project` で一元管理しています。

- **直接編集しないでください**
- 修正が必要な場合は `../py-project` を使って更新してください
- 変更を行う前に、何を変更したいのかを説明し、確認を取ってください

対象ファイル例:

- `pyproject.toml`
- `.pre-commit-config.yaml`
- `.gitlab-ci.yml`
- その他の共通設定ファイル

### ドキュメント更新の検討

コードを更新した際は、以下のドキュメントを更新する必要がないか検討してください：

- `README.md`: ユーザー向けの使用方法、機能説明
- `CLAUDE.md`: 開発ガイドライン、アーキテクチャ説明

特に以下の変更時は更新を検討：

- 新しい API エンドポイントの追加
- 新しいグラフタイプの追加
- アーキテクチャの変更
- 依存関係の大きな変更

### セキュリティ考慮事項

- `config.yaml` にはデータベースパスワードや Slack トークンが含まれるため、リポジトリにコミットしないこと
- `.gitignore` で `config.yaml` が除外されていることを確認
- 認証情報やトークンをコードにハードコードしない

## テスト

### テスト構成

- `tests/unit/`: ユニットテスト（純粋関数のテスト）
- `tests/integration/`: 統合テスト（DB 接続、グラフ生成）
- `tests/e2e/`: E2E テスト（Playwright ブラウザテスト）

### テスト設定

- タイムアウト: 300 秒
- 並列実行: auto（CPU コア数に応じて）
- カバレッジレポート: `reports/coverage/`
- HTML レポート: `reports/pytest.html`

### フィクスチャ

`tests/conftest.py` で共通フィクスチャを定義：

- `config_dict`: YAML 設定（dict 形式）
- `config`: Config dataclass インスタンス
- `env_mock`: 環境変数モック（TEST=true）
- `slack_mock`: Slack API モック

## コードパターン

### インポートスタイル

`from xxx import yyy` は基本的に使用せず、`import xxx` としてモジュールをインポートし、参照時は `xxx.yyy` と完全修飾名で記述する：

```python
# 推奨
import modes.database_postgresql

conn = modes.database_postgresql.open(...)

# 非推奨
from modes.database_postgresql import open

conn = open(...)
```

これにより、関数やクラスがどのモジュールに属しているかが明確になり、コードの可読性と保守性が向上する。

### 型アノテーションと型情報のないライブラリ

型情報を持たないライブラリを使用する場合、大量の `# type: ignore[union-attr]` を記載する代わりに、変数に `Any` 型を明示的に指定する：

```python
from typing import Any

# 推奨: Any 型を明示して type: ignore を不要にする
result: Any = some_untyped_lib.call()
result.method1()
result.method2()

# 非推奨: 大量の type: ignore コメント
result = some_untyped_lib.call()  # type: ignore[union-attr]
result.method1()  # type: ignore[union-attr]
result.method2()  # type: ignore[union-attr]
```

これにより、コードの可読性を維持しつつ型チェッカーのエラーを抑制できる。

### pyright エラーへの対処方針

pyright のエラー対策として、各行に `# type: ignore` コメントを記載して回避するのは**最後の手段**とする。

**優先順位：**

1. **型推論できるようにコードを修正する** - 変数の初期化時に型が明確になるようにする
2. **型アノテーションを追加する** - 関数の引数や戻り値、変数に適切な型を指定する
3. **Any 型を使用する** - 型情報のないライブラリの場合（上記セクション参照）
4. **`# type: ignore` コメント** - 上記で解決できない場合の最終手段

```python
# 推奨: 型推論可能なコード
items: list[str] = []
items.append("value")

# 非推奨: type: ignore の多用
items = []  # type: ignore[var-annotated]
items.append("value")  # type: ignore[union-attr]
```

**例外：** テストコードでは、モックやフィクスチャの都合上 `# type: ignore` の使用を許容する。

### dataclass の活用

設定やデータ構造には dataclass を積極的に使用：

```python
from dataclasses import dataclass, field

# 設定は frozen で不変に
@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str

# データ構造はデフォルト値を活用
@dataclass
class PreparedData:
    count: int
    times: numpy.ndarray
    altitudes: numpy.ndarray
    wind_x: numpy.ndarray = field(default_factory=lambda: numpy.array([]))
```

### キャッシング

`functools.cache` を使用してシンプルにキャッシュ：

```python
import functools

@functools.cache
def get_git_commit_hash() -> str:
    """Git commit ハッシュを取得（自動キャッシュ）"""
    result = subprocess.run(["git", "rev-parse", "HEAD"], ...)
    return result.stdout.strip()[:12]
```

### 時刻取得の統一

現在時刻の取得には `my_lib.time.now()` を使用する。`datetime.now(UTC)` の直接使用は避ける：

```python
# 推奨
import my_lib.time

now = my_lib.time.now()

# 非推奨
from datetime import UTC, datetime

now = datetime.now(UTC)
```

これにより、テスト時のモック化が容易になり、タイムゾーン管理が一元化される。

### 関数名・変数名の言語

関数名・変数名は**英語**で記述する。日本語は使用しない:

```python
# 推奨
def _try_altitude_interpolation_from_buffer():
    altitude_result = ...

# 非推奨
def _try_altitude_補完_from_buffer():
    buffer_補完 = ...
```

ただし、ログメッセージやコメントは日本語で記述してよい。

### time.time() と my_lib.time.now() の使い分け

| 用途                             | 使用する関数        |
| -------------------------------- | ------------------- |
| 観測データのタイムスタンプ       | `my_lib.time.now()` |
| ログの時刻                       | `my_lib.time.now()` |
| ジョブ管理（開始時刻、完了時刻） | `time.time()`       |
| キャッシュ TTL の計算            | `time.time()`       |
| タイムアウト判定                 | `time.time()`       |

`datetime` が必要な場面では `my_lib.time.now()`、UNIX タイムスタンプ（float）で十分な場面では `time.time()` を使用する。

### pyModeS 等のライブラリ戻り値の型処理

pyModeS のように戻り値が `T | None` のタプルを返すライブラリでは、`all(v is not None for v in ...)` チェック後も型が絞り込まれない。このような場合は `# type: ignore[arg-type]` を許容する:

```python
if all(v is not None for v in (trackangle, groundspeed, trueair)):
    # タプルアンパックでは型が絞り込まれないため type: ignore が必要
    trackangle_f = float(trackangle)  # type: ignore[arg-type]
```

ただし、CLAUDE.md「pyright エラーへの対処方針」に従い、可能な場合は型アノテーションを追加して type: ignore を回避する。

### ファイルパスと base_dir の扱い

設定ファイルで指定される相対パスは `Config.base_dir` を基準に解決する:

```python
# 推奨: base_dir を基準に解決
liveness_file = config.base_dir / config.liveness.file.collector

# 非推奨: 現在の作業ディレクトリを仮定
liveness_file = pathlib.Path.cwd() / config.liveness.file.collector
```

`base_dir` は設定読み込み時の作業ディレクトリが自動設定される。アプリケーション起動後に作業ディレクトリが変更されても正しく動作する。

### Literal 型の活用

文字列の列挙型は `Literal` 型を使用して型安全性を確保する：

```python
from typing import Literal

MethodType = Literal["mode-s", "vdl2"]
DataSourceType = Literal["bds44", "bds50_60", "acars_wn", "acars_wx", ""]

@dataclass
class WeatherObservation:
    method: MethodType = "mode-s"
    data_source: DataSourceType = ""
```

### TypedDict と dataclass の使い分け

- **dataclass**: 構造化データ、設定、ドメインオブジェクトに使用
- **TypedDict**: 外部 API のレスポンスや JSON パース結果など、dict として扱う必要がある場合に使用

パーサー関数の戻り値など、内部で使用するデータ構造には dataclass を優先する：

```python
# 推奨: パーサーの戻り値は dataclass
@dataclass
class ParsedWeatherData:
    latitude: float | None = None
    temperature_c: float | None = None

def parse_weather(msg: str) -> ParsedWeatherData | None:
    ...

# 非推奨: dict を返す
def parse_weather(msg: str) -> dict[str, Any] | None:
    ...
```

### 内部データ構造の型定義

関数間でデータを受け渡す際は、`dict` ではなく dataclass または NamedTuple を使用する：

```python
# 推奨: dataclass で型を明確に
@dataclass
class FetchResult:
    data: numpy.ndarray
    count: int

def fetch_data() -> FetchResult:
    return FetchResult(data=arr, count=len(arr))

# 推奨: 複数値を返す場合は NamedTuple
from typing import NamedTuple

class AltitudeResult(NamedTuple):
    altitude_m: float
    latitude: float | None
    longitude: float | None
    source: str

def get_altitude() -> AltitudeResult | None:
    return AltitudeResult(altitude_m=10000, latitude=35.0, longitude=139.0, source="adsb")

# 非推奨: dict だと型が不明確
def fetch_data() -> dict[str, Any]:
    return {"data": arr, "count": len(arr)}
```

API レスポンスなど外部向けの辞書は TypedDict を使用する：

```python
from typing import TypedDict

class JobStatusDict(TypedDict):
    """API レスポンス用ジョブステータス"""
    job_id: str
    status: str
    progress: int

def get_status() -> JobStatusDict:
    return {"job_id": "xxx", "status": "completed", "progress": 100}
```

### 後方互換性コードの扱い

本番で使用されなくなったコードは、ファイル先頭の docstring に非推奨マークを追加する：

```python
"""SQLite データベースアクセス（開発・テスト用）

DEPRECATED: 本番環境では postgresql.py を使用してください。
このファイルは開発・テスト用途のみでサポートされます。
"""
```

### モジュールレベル定数の活用

複数箇所で使用される定数値（特にバリデーション用のリスト等）はモジュールレベルで定義し、重複を排除する:

```python
# 推奨: モジュールレベルで定数として定義
VALID_COLUMNS: tuple[str, ...] = ("time", "altitude", "temperature", ...)

def fetch_data(columns: list[str]) -> ...:
    sanitized = [c for c in columns if c in VALID_COLUMNS]

# 非推奨: 関数内で毎回定義
def fetch_data(columns: list[str]) -> ...:
    valid_columns = ["time", "altitude", "temperature", ...]  # 重複
    sanitized = [c for c in columns if c in valid_columns]
```

定数名は `UPPER_SNAKE_CASE` で記述する。

### 型定義の再利用

同じ構造の dataclass が複数ファイルに存在する場合、`core/types.py` の定義を再利用する:

```python
# 推奨: 既存の型定義を再利用
from amdar.core.types import WindData

# 非推奨: 同じ構造を再定義
@dataclass
class WindData:
    x: float
    y: float
    ...
```

### シングルトンパターンの型安全な実装

シングルトンパターンでは `# type: ignore` を避け、assert で型を絞り込む：

```python
from typing import ClassVar

# 推奨: assert で型を絞り込む
class MySingleton:
    _instance: ClassVar[MySingleton | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    def __new__(cls) -> MySingleton:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        assert cls._instance is not None
        return cls._instance

# 非推奨: type: ignore で回避
def __new__(cls) -> Self:
    ...
    return cls._instance  # type: ignore[return-value]
```

注意: 戻り値の型には `Self` ではなく具体的なクラス名を使用する。`Self` はサブクラス化を前提とした型であり、シングルトンでは不適切。

### 共通定数の集約

複数ファイルで使用する定数は `amdar/constants.py` に集約する：

```python
# 推奨: 共通モジュールで定義
import amdar.constants

schema_path = amdar.constants.get_schema_path()

# 非推奨: 各ファイルで重複定義
_SCHEMA_CONFIG = "config.schema"
```

### 設定読み込みの統一

設定ファイルの読み込みには `amdar.config.load_config()` を使用する：

```python
# 推奨: ヘルパー関数を使用
import amdar.config

config = amdar.config.load_config(config_file)

# 非推奨: 個別にロードと変換
config_dict = my_lib.config.load(config_file, amdar.constants.get_schema_path())
config = amdar.config.load_from_dict(config_dict, pathlib.Path.cwd())
```

`load_config()` はスキーマ検証を含み、`base_dir` を現在の作業ディレクトリに設定する。
`config_dict` を直接使用する必要がある場合（sqlite.py など）のみ、`load_from_dict()` を使用する。

### スキーマファイルパスの管理

データベーススキーマファイルのパスは `constants.py` の関数を使用する：

```python
# 推奨: constants.py の関数を使用
from amdar.constants import get_db_schema_path

schema_path = get_db_schema_path("postgres.schema")

# 非推奨: 各ファイルで __file__ からの相対パス計算
_SCHEMA_FILE = pathlib.Path(__file__).parent.parent.parent.parent / "schema" / "postgres.schema"
```

### マジックナンバーの constants.py 集約

複数箇所で使用される定数値は `constants.py` に定義する：

```python
# 推奨: 定数として定義
from amdar.constants import DEFAULT_DISTANCE_KM

fetch_data(distance=DEFAULT_DISTANCE_KM)

# 非推奨: ハードコード
fetch_data(distance=100)
```

### 単位変換定数の使用

単位変換には `constants.py` の定数を使用する：

```python
# 推奨: constants.py の定数を使用
import amdar.constants

altitude_m = altitude_ft * amdar.constants.FEET_TO_METERS
speed_ms = speed_kt * amdar.constants.KNOTS_TO_MS

# 非推奨: マジックナンバー
altitude_m = altitude_ft * 0.3048
speed_ms = speed_kt * 0.514444
```

主な単位変換定数：

| 定数名                   | 値       | 用途                |
| ------------------------ | -------- | ------------------- |
| `FEET_TO_METERS`         | 0.3048   | フィート → メートル |
| `METERS_TO_FEET`         | 3.28084  | メートル → フィート |
| `KNOTS_TO_MS`            | 0.514444 | ノット → m/s        |
| `KM_PER_DEGREE_LATITUDE` | 111.0    | 緯度1度あたりのkm   |

### タイムアウト定数の管理

タイムアウトやインターバルなどの時間関連定数は `constants.py` で一元管理する：

```python
# 推奨: constants.py で定義した値を使用
from amdar.constants import CACHE_TTL_SECONDS

cache_ttl = CACHE_TTL_SECONDS

# 非推奨: 各ファイルでマジックナンバー
cache_ttl = 1800
```

主な定数：

| 定数名                           | 値   | 用途                 |
| -------------------------------- | ---- | -------------------- |
| `CACHE_TTL_SECONDS`              | 1800 | キャッシュの有効期限 |
| `JOB_TIMEOUT_SECONDS`            | 1200 | ジョブタイムアウト   |
| `JOB_EXPIRY_SECONDS`             | 1800 | ジョブ結果の保持期間 |
| `PREGENERATION_INTERVAL_SECONDS` | 1500 | 事前生成の間隔       |

### モジュール変数の初期化

未初期化状態を明示するため、`Path()` ではなく `None` を使用する：

```python
# 推奨: 未初期化を明示
_liveness_file: pathlib.Path | None = None

def initialize(path: pathlib.Path) -> None:
    global _liveness_file
    _liveness_file = path

def update() -> None:
    if _liveness_file is not None:
        my_lib.footprint.update(_liveness_file)

# 非推奨: 空パスで初期化
_liveness_file: pathlib.Path = pathlib.Path()
```

### Protocol と TypedDict/dataclass の使い分け

Protocol は複数の異なる実装が同じインターフェースを共有する場合にのみ使用する。
単一の実装に対しては TypedDict または dataclass を優先する：

```python
# 推奨: 単一実装には dataclass
@dataclass
class MessageFragment:
    icao: str
    altitude_ft: float | None = None

# 非推奨: 不要な Protocol 抽象化
class FragmentProtocol(Protocol):
    icao: str
    altitude_ft: float | None
```

### ParsedData と DomainObject の分離

パーサーの戻り値（ParsedData）とドメインオブジェクトは別々に定義する：

```python
# パーサー戻り値: 解析結果のみ
@dataclass
class ParsedWeatherData:
    temperature_c: float | None = None
    wind_speed_kt: int | None = None

# ドメインオブジェクト: ビジネスロジックを含む
@dataclass
class WeatherObservation:
    temperature: float | None = None
    wind: WindData | None = None

    def is_valid(self) -> bool:
        ...
```

### Option パターンは使用しない

Python では `| None` パターンが標準。Rust/Scala スタイルの Option 型ラッパーは使用しない：

```python
# 推奨: Python 標準の None パターン
value: str | None = None
if value is not None:
    process(value)

# 非推奨: Option 型ラッパー
value: Option[str] = Option(None)
if value.is_some():
    process(value.unwrap())
```

### コード重複の排除

同じロジックが複数箇所に存在する場合は、共通関数に抽出する：

```python
# 推奨: 共通関数に抽出
def _convert_rows_to_numpy_arrays(rows, include_wind):
    # 変換ロジック
    ...

result1 = _convert_rows_to_numpy_arrays(rows1, include_wind=True)
result2 = _convert_rows_to_numpy_arrays(rows2, include_wind=False)

# 非推奨: 同じロジックを複数箇所に記述
for i, row in enumerate(rows1):
    times[i] = row[0]
    ...  # 変換ロジック

for i, row in enumerate(rows2):
    times[i] = row[0]
    ...  # 同じ変換ロジック（重複）
```

### naive datetime 変換の統一

PostgreSQL との連携で naive datetime への変換が必要な場合は、`_to_naive_datetime()` ヘルパー関数を使用する：

```python
# 推奨: 専用関数を使用
naive_dt = _to_naive_datetime(dt)

# 非推奨: 毎回 replace を呼び出す
naive_dt = dt.replace(tzinfo=None)
```

これにより、変換の目的がコードから明確になり、変換ロジック変更時に1箇所で対応できる。

### カラムサニタイズの共通化

SQL カラム名のサニタイズには `amdar.constants.sanitize_columns()` を使用する：

```python
# 推奨: constants.py の関数を使用
import amdar.constants

columns_str = amdar.constants.sanitize_columns(columns, VALID_METEOROLOGICAL_COLUMNS)

# 非推奨: 各ファイルで重複実装
sanitized = [col for col in columns if col in VALID_COLUMNS]
if not sanitized:
    raise ValueError("No valid columns")
columns_str = ", ".join(sanitized)
```

### my_lib.git_util の活用

Git 情報の取得には `my_lib.git_util` を使用する：

```python
# 推奨: my_lib.git_util を使用
import my_lib.git_util

revision = my_lib.git_util.get_revision_info()
commit_hash = revision.hash[:12]

# 非推奨: subprocess で直接実行
result = subprocess.run(["git", "rev-parse", "HEAD"], ...)
```

### 距離計算の共通化

距離計算は `amdar.core.geo` モジュールの共通関数を使用する：

```python
# 推奨: 共通モジュールを使用
import amdar.core.geo

# 簡易計算（高速、近距離向け）
distance = amdar.core.geo.simple_distance(lat, lon, ref_lat, ref_lon)

# Haversine 公式（精密、長距離向け）
distance = amdar.core.geo.haversine_distance(ref_lat, ref_lon, lat, lon)

# 非推奨: 各ファイルで独自実装
lat_dist = (lat - ref_lat) * 111.0
lon_dist = (lon - ref_lon) * 111.0 * math.cos(math.radians(ref_lat))
distance = math.sqrt(lat_dist**2 + lon_dist**2)
```

精度が必要な場合は `haversine_distance` を、パフォーマンスを優先する場合は `simple_distance` を使用する。

### 単位変換定数の使用

単位変換には `constants.py` の定数を使用する：

```python
# 推奨: constants.py の定数を使用
import amdar.constants

altitude_m = altitude_ft * amdar.constants.FEET_TO_METERS
speed_ms = speed_kt * amdar.constants.KNOTS_TO_MS

# 非推奨: マジックナンバー
altitude_m = altitude_ft * 0.3048
speed_ms = speed_kt * 0.514444
```

主な単位変換定数：

| 定数名                   | 値       | 用途                |
| ------------------------ | -------- | ------------------- |
| `FEET_TO_METERS`         | 0.3048   | フィート → メートル |
| `METERS_TO_FEET`         | 3.28084  | メートル → フィート |
| `KNOTS_TO_MS`            | 0.514444 | ノット → m/s        |
| `KM_PER_DEGREE_LATITUDE` | 111.0    | 緯度1度あたりのkm   |

### タイムアウト定数の管理

タイムアウトやインターバルなどの時間関連定数は `constants.py` で一元管理する：

```python
# 推奨: constants.py で定義した値を使用
from amdar.constants import CACHE_TTL_SECONDS

cache_ttl = CACHE_TTL_SECONDS

# 非推奨: 各ファイルでマジックナンバー
cache_ttl = 1800
```

主な定数：

| 定数名                           | 値   | 用途                 |
| -------------------------------- | ---- | -------------------- |
| `CACHE_TTL_SECONDS`              | 1800 | キャッシュの有効期限 |
| `JOB_TIMEOUT_SECONDS`            | 1200 | ジョブタイムアウト   |
| `JOB_EXPIRY_SECONDS`             | 1800 | ジョブ結果の保持期間 |
| `PREGENERATION_INTERVAL_SECONDS` | 1500 | 事前生成の間隔       |

### singledispatch とパターンマッチングの使用基準

`isinstance` チェックが3つ以上連続する場合のみ、singledispatch やパターンマッチングへの置き換えを検討する。
2つ以下の場合は、既存の `isinstance` チェックの方が可読性が高い：

```python
# 2つ以下の isinstance は OK
if isinstance(value, str):
    return process_string(value)
elif isinstance(value, int):
    return process_int(value)
return value

# 3つ以上の場合はパターンマッチングを検討
match value:
    case str():
        return process_string(value)
    case int():
        return process_int(value)
    case float():
        return process_float(value)
    case _:
        return value
```

### ログレベルの使い分け

Python の logging モジュールを適切に使用する。プレフィックスでログレベルを示すのではなく、適切なログレベルを指定する：

```python
# 推奨: 適切なログレベルを使用
logging.debug("詳細なデバッグ情報: value=%s", value)
logging.info("処理開始/完了などの通常情報")
logging.warning("警告: 問題があるが処理は継続")
logging.error("エラー: 処理が失敗")

# 例外のスタックトレースを debug レベルで出力
try:
    ...
except Exception:
    logging.debug("パース失敗", exc_info=True)

# 非推奨: info レベルに [DEBUG] プレフィックスを付ける
logging.info("[DEBUG] 詳細情報: value=%s", value)
```

| レベル    | 用途                                               |
| --------- | -------------------------------------------------- |
| `DEBUG`   | 開発時のみ必要な詳細情報（パラメータ値、中間結果） |
| `INFO`    | 処理の開始/完了、重要なマイルストーン              |
| `WARNING` | 問題はあるが処理は継続できる状況                   |
| `ERROR`   | 処理が失敗した状況                                 |

### リファクタリング時の検討観点

コードをリファクタリングする際は、以下の観点で改善の余地を検討する。
ただし、**デメリットがメリットを上回る場合は実施しない**。

1. **型整備**
    - Protocol の導入は複数の異なる実装が同じインターフェースを共有する場合のみ
    - `isinstance` チェックが3つ以上連続する場合のみ singledispatch/パターンマッチングを検討
    - 内部データ構造には dict より dataclass を優先

2. **定数管理**
    - ローカルでのエイリアス定義は避け、constants.py を直接参照
    - 後方互換性のためのエイリアスは docstring にその旨を明記

3. **パス解決**
    - `pathlib.Path.cwd()` より `__file__` ベースのパス解決を優先
    - 設定ファイルのパスは config.base_dir を使用

4. **my_lib の活用**
    - time.time() と my_lib.time.now() の使い分けルールを遵守
    - 新規機能追加時は my_lib に同様の機能がないか確認

### リファクタリング調査時の判断基準

改善候補を検討する際は、工数対効果を評価する。

1. **Protocol 導入の判断**
    - 複数の異なる実装が同じインターフェースを共有する場合のみ導入
    - 単一実装や deprecated なモジュール（sqlite.py 等）には不要

2. **TypedDict → dataclass 置き換えの判断**
    - 外部 API レスポンスや JSON パース結果: TypedDict を維持
    - 内部ドメインオブジェクト: dataclass を優先
    - 動的キー設定（dict["key"] = value）が多用される場合は TypedDict を維持
    - 大規模変更が必要な場合は工数対効果を検討

3. **計算ロジック統合の判断**
    - 完全に同一のロジックが複数箇所にある場合: 共通モジュールに抽出
    - 類似しているが微妙に異なる場合: 各コンテキストで維持

4. **定数集約の判断**
    - 複数ファイルで使用される定数: constants.py に集約
    - 単一ファイル内でのみ使用: ローカル定数として維持

5. **パス解決の統一**
    - スキーマファイル等の固定パス: constants.py の関数を使用
    - 設定ファイルからの相対パス: config.base_dir を使用
    - 作業ディレクトリ依存のコードは避ける

## API エンドポイント

### グラフ生成（非同期）

```
POST /modes-sensing/api/graph/job
Content-Type: application/json

{
  "graph_name": "scatter_2d",
  "start": "2025-01-01T00:00:00Z",
  "end": "2025-01-07T00:00:00Z",
  "limit_altitude": false
}

Response: {"job_id": "uuid"}
```

```
GET /modes-sensing/api/graph/job/{job_id}/status

Response: {"status": "completed", "progress": 100, ...}
```

```
GET /modes-sensing/api/graph/job/{job_id}/result

Response: PNG 画像
```

### データ情報

```
GET /modes-sensing/api/data-range

Response: {"earliest": "...", "latest": "...", "count": 12345}
```

```
GET /modes-sensing/api/aggregate-stats

Response: {"meteorological_data": {...}, ...}
```

## 外部依存

### Python

- **pyModeS**: Mode S メッセージデコード
- **psycopg2-binary**: PostgreSQL ドライバー
- **numpy / pandas / scipy**: データ処理
- **scikit-learn**: 機械学習（外れ値検出）
- **matplotlib / Pillow**: グラフ生成
- **flask / flask-cors**: Web フレームワーク
- **my-lib**: 作者の共通ライブラリ（git 経由）

### Node.js

- **react / react-dom**: UI フレームワーク
- **vite**: ビルドツール
- **bulma**: CSS フレームワーク
- **dayjs**: 日付処理

## ライセンス

Apache License Version 2.0

## 開発ワークフロー規約

### コミット時の注意

- 今回のセッションで作成し、プロジェクトが機能するのに必要なファイル以外は git add しないこと
- 気になる点がある場合は追加して良いか質問すること

### バグ修正の原則

- 憶測に基づいて修正しないこと
- 必ず原因を論理的に確定させた上で修正すること
- 「念のため」の修正でコードを複雑化させないこと

### コード修正時の確認事項

- 関連するテストも修正すること
- 関連するドキュメントも更新すること
- mypy, pyright, ty がパスすることを確認すること
