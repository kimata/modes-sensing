-- ============================================================
-- modes-sensing データベース移行 SQL
--
--   1. time カラムの TIMESTAMP → TIMESTAMPTZ 移行
--   2. マテリアライズドビュー → 増分集約テーブルへの置き換え
--   3. 冗長インデックスの削除
--
-- 実行方法:
--   psql -h <host> -p <port> -U <user> -d <database> \
--       -f scripts/migrate_timestamptz_and_aggregates.sql
--
-- 注意:
--   * 収集プロセス（amdar）と Web サーバー（amdar-webui）を停止してから実行すること。
--   * Step 2 の ALTER TABLE はテーブル全体の書き換えを伴い、
--     ACCESS EXCLUSIVE ロックを取得する。数百万行規模では数分〜数十分かかる
--     可能性がある（テーブルサイズの約2倍の空きディスク容量も必要）。
--   * Step 5 の初期投入も全件スキャンを伴うため、数分かかる可能性がある。
--   * このスクリプトは冪等に作られており、途中失敗後の再実行が可能
--     （Step 1〜5 は単一トランザクションのため、失敗時は全体がロールバックされる）。
-- ============================================================

BEGIN;

-- ------------------------------------------------------------
-- Step 1: 既存マテリアライズドビューの削除
-- 増分集約テーブルに置き換えるため、ビュー本体と付随インデックスを削除する。
-- 移行済み（同名の通常テーブルが存在する）の場合は何もしない（冪等）。
-- ------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'halfhourly_altitude_grid') THEN
        EXECUTE 'DROP MATERIALIZED VIEW halfhourly_altitude_grid CASCADE';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = 'threehour_altitude_grid') THEN
        EXECUTE 'DROP MATERIALIZED VIEW threehour_altitude_grid CASCADE';
    END IF;
END
$$;

-- ------------------------------------------------------------
-- Step 2: time カラムの TIMESTAMPTZ 化
-- 既存の naive 値はサーバーローカルタイム（JST）として解釈して変換する。
-- 既に TIMESTAMPTZ の場合は何もしない（冪等）。
-- ------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'meteorological_data'
          AND column_name = 'time'
          AND data_type = 'timestamp without time zone'
    ) THEN
        ALTER TABLE meteorological_data
            ALTER COLUMN time TYPE TIMESTAMPTZ
            USING time AT TIME ZONE 'Asia/Tokyo';
    END IF;
END
$$;

-- ------------------------------------------------------------
-- Step 3: 不要インデックスの削除
--   idx_time                   : idx_time_alt のプレフィックスで冗長
--   idx_altitude               : 使用するクエリなし
--   idx_time_distance_temp_alt : 主要クエリの述語と不一致で未使用の可能性大
-- 適用前に EXPLAIN や pg_stat_user_indexes.idx_scan で未使用であることを
-- 確認可能。idx_time_alt と idx_time_brin は維持する。
-- ------------------------------------------------------------
DROP INDEX IF EXISTS idx_time;
DROP INDEX IF EXISTS idx_altitude;
DROP INDEX IF EXISTS idx_time_distance_temp_alt;

-- ------------------------------------------------------------
-- Step 4: 増分集約テーブルの作成
-- schema/postgres.schema と同じ定義。
-- time_bucket は既存データとの連続性のため JST（Asia/Tokyo）基準で計算する。
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS halfhourly_altitude_grid (
    time_bucket TIMESTAMPTZ NOT NULL,
    altitude_bin INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    altitude REAL,
    temperature REAL,
    wind_x REAL,
    wind_y REAL,
    wind_speed REAL,
    wind_angle REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_halfhourly_grid_unique
ON halfhourly_altitude_grid (time_bucket, altitude_bin);

CREATE TABLE IF NOT EXISTS threehour_altitude_grid (
    time_bucket TIMESTAMPTZ NOT NULL,
    altitude_bin INTEGER NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    altitude REAL,
    temperature REAL,
    wind_x REAL,
    wind_y REAL,
    wind_speed REAL,
    wind_angle REAL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_threehour_grid_unique
ON threehour_altitude_grid (time_bucket, altitude_bin);

-- ------------------------------------------------------------
-- Step 5: 集約テーブルへの初期全量投入
-- 旧マテリアライズドビューと同じ SELECT（品質フィルタ含む）。
-- TRUNCATE してから投入するため再実行しても重複しない（冪等）。
-- ------------------------------------------------------------
TRUNCATE halfhourly_altitude_grid;

INSERT INTO halfhourly_altitude_grid
    (time_bucket, altitude_bin, time, altitude, temperature,
     wind_x, wind_y, wind_speed, wind_angle)
SELECT DISTINCT ON (time_bucket, altitude_bin)
    (date_trunc('hour', time AT TIME ZONE 'Asia/Tokyo')
        + (floor(EXTRACT(minute FROM time AT TIME ZONE 'Asia/Tokyo') / 30)
            * interval '30 minutes')) AT TIME ZONE 'Asia/Tokyo' AS time_bucket,
    (floor(altitude / 250) * 250)::int AS altitude_bin,
    time,
    altitude,
    temperature,
    wind_x,
    wind_y,
    wind_speed,
    wind_angle
FROM meteorological_data
WHERE distance <= 100
  AND temperature > -100
  AND altitude IS NOT NULL
  AND altitude >= 0
  AND altitude <= 13000
ORDER BY time_bucket, altitude_bin, time DESC;

TRUNCATE threehour_altitude_grid;

INSERT INTO threehour_altitude_grid
    (time_bucket, altitude_bin, time, altitude, temperature,
     wind_x, wind_y, wind_speed, wind_angle)
SELECT DISTINCT ON (time_bucket, altitude_bin)
    (date_trunc('hour', time AT TIME ZONE 'Asia/Tokyo')
        - (mod(EXTRACT(hour FROM time AT TIME ZONE 'Asia/Tokyo')::int, 3)
            * interval '1 hour')) AT TIME ZONE 'Asia/Tokyo' AS time_bucket,
    (floor(altitude / 250) * 250)::int AS altitude_bin,
    time,
    altitude,
    temperature,
    wind_x,
    wind_y,
    wind_speed,
    wind_angle
FROM meteorological_data
WHERE distance <= 100
  AND temperature > -100
  AND altitude IS NOT NULL
  AND altitude >= 0
  AND altitude <= 13000
ORDER BY time_bucket, altitude_bin, time DESC;

COMMIT;

-- ------------------------------------------------------------
-- Step 6: 統計情報の更新
-- プランナが新しい型・テーブルに対して適切な実行計画を選べるようにする。
-- ------------------------------------------------------------
ANALYZE meteorological_data;
ANALYZE halfhourly_altitude_grid;
ANALYZE threehour_altitude_grid;
