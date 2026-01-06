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
├── collect_combined.py         # Mode S + VDL2 統合収集
├── collect_vdl2.py             # VDL2 のみ収集（デバッグ用）
└── amdar/
    ├── __main__.py             # amdar コマンド (Mode S 収集)
    ├── cli_collect.py          # amdar エントリポイント実装
    ├── cli_webui.py            # amdar-webui (Flask Web サーバー)
    ├── cli_healthz.py          # amdar-healthz (ヘルスチェック)
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

react/                          # React フロントエンド
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

| 期間     | 集約       | 用途             |
| -------- | ---------- | ---------------- |
| 7日以内  | 生データ   | リアルタイム分析 |
| 7-30日   | 1時間×500m | 中期分析         |
| 30日以上 | 6時間×500m | 長期トレンド     |

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
