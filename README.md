# ✈️ modes-sensing

航空機から送信される Mode S メッセージを受信し、気象データを可視化するシステム

[\![Regression](https://github.com/kimata/modes-sensing/actions/workflows/regression.yaml/badge.svg)](https://github.com/kimata/modes-sensing/actions/workflows/regression.yaml)

## 📋 概要

航空機が送信する Mode S メッセージ（BDS 4,4 および BDS 4,5）から気象データ（気温・風速・風向）を抽出し、可視化するシステムです。高度別の大気状態をリアルタイムで観測できます。

### 主な特徴

- ✈️ **リアルタイム受信** - Mode S メッセージをリアルタイムで受信・デコード
- 🌡️ **気象データ抽出** - 航空機から送信される気温・風速・風向データを取得
- 📊 **多彩な可視化** - 2D/3D 散布図、ヒートマップ、等高線プロットなど
- 🗄️ **データベース保存** - PostgreSQL/SQLite による長期データ保存
- 📅 **期間選択** - 過去24時間、7日間、1ヶ月間、カスタム期間での表示
- 🚀 **高速処理** - カラム選択による最適化されたデータベースアクセス
- 📱 **レスポンシブUI** - スマートフォンからPCまで対応

## 🚀 セットアップ

### 必要な環境

- Python 3.10+
- Node.js 18.x 以上
- PostgreSQL 14+ (または SQLite)
- RTL-SDR と dump1090 (Mode S 受信用)

### 依存パッケージのインストール

```bash
# システムパッケージ
sudo apt install postgresql postgresql-contrib
sudo apt install rtl-sdr dump1090-mutability

# Python環境（uvを使用）
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync

# React環境
cd react
npm ci
```

## 💻 実行方法

### データ収集の開始

```bash
# Mode S メッセージの収集開始
uv run python src/collect.py

# デバッグモードで実行
uv run python src/collect.py -D
```

### Web インターフェースの起動

```bash
# React アプリのビルド
cd react
npm run build
cd ..

# Flask サーバーの起動
uv run python src/app.py
```

ブラウザで http://localhost:5000 にアクセス

## 📊 グラフの種類

| グラフタイプ | 説明                           | 適用場面                 |
| ------------ | ------------------------------ | ------------------------ |
| 2D散布図     | 時間-高度-温度の関係を点で表示 | 全体的な傾向の把握       |
| 3D散布図     | 時間-高度-温度を3次元で表示    | 立体的なデータ分布の確認 |
| ヒートマップ | 格子状に補間した温度分布       | 連続的な温度変化の可視化 |
| 2D等高線     | 等温線による表示               | 温度層の境界確認         |
| 3D等高線     | 3次元の等温面表示              | 複雑な温度構造の把握     |
| 密度プロット | 高度-温度の分布密度            | データの集中度分析       |

## 📝 ライセンス

このプロジェクトは Apache License Version 2.0 のもとで公開されています。
EOF < /dev/null
