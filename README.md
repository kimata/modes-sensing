# modes-sensing

航空機から送信される Mode S / VDL2 メッセージを受信し、気象データを可視化するシステム

[![Test Status](https://github.com/kimata/modes-sensing/actions/workflows/test.yaml/badge.svg)](https://github.com/kimata/modes-sensing/actions/workflows/test.yaml)
[![Test Report](https://img.shields.io/badge/Test_Report-pytest.html-blue)](https://kimata.github.io/modes-sensing/pytest.html)
[![Coverage Status](https://coveralls.io/repos/github/kimata/modes-sensing/badge.svg?branch=main)](https://coveralls.io/github/kimata/modes-sensing?branch=main)

## 目次

- [概要](#概要)
- [アーキテクチャ](#アーキテクチャ)
- [セットアップ](#セットアップ)
- [実行方法](#実行方法)
- [設定ファイル](#設定ファイル)
- [API エンドポイント](#api-エンドポイント)
- [グラフの種類](#グラフの種類)
- [テスト](#テスト)
- [トラブルシューティング](#トラブルシューティング)
- [ライセンス](#ライセンス)

## 概要

航空機が送信する SSR Mode S メッセージ（BDS 4,4 / BDS 5,0 / BDS 6,0）および VDL2 ACARS メッセージから気象データ（気温・風速・風向）を抽出し、可視化するシステムです。

### 主な特徴

- **デュアルデータソース** - Mode S (1090MHz) と VDL2 (136MHz) の両方に対応
- **リアルタイム受信** - 航空機からのメッセージをリアルタイムで受信・デコード
- **高度補完** - VDL2 データの不足高度を ADS-B データで補完
- **外れ値検出** - 高度-温度相関（線形回帰 + 高度近傍統計）による異常値自動除去
- **多彩な可視化** - 10種類のグラフタイプ（2D/3D 散布図、ヒートマップ、等高線、鉛直プロファイルなど）
- **非同期グラフ生成** - マルチプロセスによる高速なグラフ生成
- **時間帯別集約** - 長期データの効率的なストレージと高速クエリ

## アーキテクチャ

システム構成・データフロー・スレッディングモデル・データベース集約戦略などの詳細は、図解付きの
**[doc/architecture.md](doc/architecture.md)** を参照してください。

概略: 収集プロセス `amdar`（Mode S: dump1090-fa / TCP :30002、VDL2: dumpvdl2 / ZMQ :5050）が
気象データを抽出して PostgreSQL に保存し、Web サーバー `amdar-webui`（Flask + React）が
非同期ジョブ + キャッシュでグラフを生成・配信します。

## セットアップ

### 必要な環境

- Python 3.11+ （推奨: 3.13）
- Node.js 18.x+
- PostgreSQL 14+
- RTL-SDR ドングル（2台：Mode S 用 + VDL2 用）
- Docker

### 1. 依存パッケージのインストール

```bash
# Python 環境
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync

# React 環境
cd frontend && npm ci
```

### 2. デコーダの準備

#### dump1090-fa（Mode S 用）

```bash
docker run -d \
  --name dump1090-fa \
  --device=/dev/bus/usb \
  --restart=unless-stopped \
  -p 30002:30002 \
  -p 8080:8080 \
  registry.gitlab.com/kimata/dump1090-fa:latest
```

#### dumpvdl2（VDL2 用）

```bash
# dumpvdl2 のインストールと起動は環境に依存
# ZMQ 出力を有効にして起動（デフォルト: tcp://*:5050）
```

## 実行方法

### データ収集

```bash
# Mode S + VDL2 統合（推奨）
uv run amdar

# VDL2 のみ（デバッグ用）
uv run python src/collect_vdl2.py
```

### Web インターフェース

```bash
# React ビルド
cd react && npm run build && cd ..

# Flask サーバー起動
uv run amdar-webui
# → http://localhost:5000
```

## 設定ファイル

`config.yaml` の構成：

```yaml
decoder:
    modes:
        host: localhost
        port: 30002 # dump1090-fa TCP ポート
    vdl2: # オプション
        host: 192.168.0.20
        port: 5050 # dumpvdl2 ZMQ ポート

database:
    host: localhost
    port: 5432
    name: flight_weather
    user: postgres
    pass: postgres

filter:
    area:
        lat:
            ref: 35.682677 # 基準緯度（東京）
        lon:
            ref: 139.762230 # 基準経度
        distance: 100 # フィルタ距離 (km)

liveness:
    file:
        collector: /dev/shm/modes-sensing/liveness/collector
        receiver:
            modes: /dev/shm/modes-sensing/liveness/modes
            vdl2: /dev/shm/modes-sensing/liveness/vdl2
    schedule:
        daytime:
            start_hour: 7
            end_hour: 22
            timeout_sec: 60 # 昼間: 1分タイムアウト
        nighttime:
            timeout_sec: 3600 # 夜間: 1時間タイムアウト

webapp:
    static_dir_path: frontend/dist
    cache_dir_path: cache

slack: # オプション
    from: ModeS sensing
    bot_token: xoxp-XXX...
    error:
        channel:
            name: "#error"
        interval_min: 60
```

## API エンドポイント

すべて `/modes-sensing` プレフィックス配下です。グラフ生成（非同期ジョブ + SSE）、データ情報、
受信品質・監視（`/api/receiver-quality`、Prometheus 形式の `/api/metrics`）などを提供します。

エンドポイントの一覧と入出力形式は [doc/architecture.md の HTTP API 一覧](doc/architecture.md#http-api-一覧) を参照してください。

## グラフの種類

| graph_name            | 説明                                | 用途               |
| --------------------- | ----------------------------------- | ------------------ |
| `scatter_2d`          | 時間-高度-温度 2D 散布図            | 全体傾向の把握     |
| `scatter_3d`          | 3次元散布図                         | 立体的データ分布   |
| `heatmap`             | 補間した温度分布                    | 連続的温度変化     |
| `contour_2d`          | 等温線                              | 温度層境界         |
| `contour_3d`          | 3次元等温面                         | 複雑な温度構造     |
| `density`             | 高度-温度分布密度                   | データ集中度分析   |
| `temperature`         | 時間-温度時系列                     | 温度変化追跡       |
| `wind_direction`      | 高度別風向・風速                    | 風パターン分析     |
| `temperature_profile` | 気温の鉛直プロファイル（末尾3時間） | 大気鉛直構造の把握 |
| `hodograph`           | 風のホドグラフ（末尾3時間）         | 風の鉛直シアー把握 |

## テスト

```bash
# 全テスト
uv run pytest

# ユニットテストのみ
uv run pytest tests/unit/

# 統合テスト
uv run pytest tests/integration/

# カバレッジレポート
uv run pytest --cov=src --cov-report=html
```

## トラブルシューティング

### RTL-SDR が認識されない

```bash
lsusb | grep RTL
sudo usermod -a -G plugdev $USER
```

### dump1090-fa 接続エラー

```bash
docker logs dump1090-fa
nc localhost 30002  # データ受信確認
```

### VDL2 データが来ない

```bash
# ZMQ 接続テスト
python -c "import zmq; ctx=zmq.Context(); s=ctx.socket(zmq.SUB); s.connect('tcp://192.168.0.20:5050'); s.setsockopt(zmq.SUBSCRIBE, b''); print(s.recv())"
```

### グラフが生成されない

1. データ範囲の確認: `/api/data-range`
2. ジョブステータスの確認: `/api/graph/job/{id}/status`
3. Flask ログでエラー確認

## ライセンス

Apache License Version 2.0

---

[Issue 報告](https://github.com/kimata/modes-sensing/issues) | [Wiki](https://github.com/kimata/modes-sensing/wiki)
