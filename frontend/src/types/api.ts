/**
 * API 型定義
 *
 * バックエンドの TypedDict/dataclass と対応するフロントエンド型定義。
 * バックエンドの変更時はこのファイルも更新すること。
 *
 * 対応ファイル:
 * - src/amdar/viewer/api/job_manager.py (JobStatusDict)
 * - src/amdar/viewer/api/graph.py (各エンドポイントのレスポンス)
 */

// ジョブステータス値（JobStatus enum と対応）
export type JobStatusValue = "pending" | "processing" | "completed" | "failed" | "timeout";

/**
 * サーバーが保持していない（不明な）ジョブのステータス
 * 対応: graph_routes.py get_jobs_status_batch / job_events
 */
export interface UnknownJobStatus {
    status: "unknown";
}

/**
 * ジョブ情報（POST /api/graph/job レスポンスの要素）
 * 対応: graph.py create_graph_job
 */
export interface JobInfo {
    job_id: string;
    graph_name: string;
}

/**
 * ジョブ作成レスポンス（POST /api/graph/job）
 * 対応: graph.py create_graph_job
 */
export interface CreateJobsResponse {
    jobs: JobInfo[];
}

/**
 * ジョブステータス情報（GET /api/graph/job/{id}/status レスポンス）
 * 対応: job_manager.py JobStatusDict
 *
 * 注意: stage フィールドはバックエンドでは `str | None`、
 * フロントエンドでは `string | undefined` として扱う
 */
export interface JobStatusInfo {
    job_id: string;
    status: JobStatusValue;
    progress: number;
    graph_name: string;
    error: string | null;
    elapsed_seconds: number | null;
    stage: string | null;
}

/**
 * 一括ステータスのジョブ情報（job_id フィールドを除く）
 * 対応: graph.py get_jobs_status_batch
 */
export interface BatchJobStatusInfo {
    status: JobStatusValue;
    progress: number;
    graph_name: string;
    error: string | null;
    elapsed_seconds: number | null;
    stage: string | null;
}

/**
 * 一括ステータスの各エントリ
 * 不明な job_id は {"status": "unknown"} として含まれる
 */
export type BatchJobStatusEntry = BatchJobStatusInfo | UnknownJobStatus;

/**
 * 一括ステータスレスポンス（GET /api/graph/jobs/status?job_ids=...）
 * 対応: graph.py get_jobs_status_batch
 */
export interface BatchStatusResponse {
    jobs: Record<string, BatchJobStatusEntry>;
}

/**
 * SSE ステータスイベントのペイロード
 * （GET /api/graph/job/events?job_ids=... の event: status）
 *
 * 各エントリは JobStatusDict（job_id を含む）または {"status": "unknown"}。
 * バッチステータスと同じフィールドを参照するため BatchJobStatusEntry として扱う。
 */
export interface SseStatusEventData {
    jobs: Record<string, BatchJobStatusEntry>;
}

/**
 * データ範囲レスポンス（GET /api/data-range）
 * 対応: graph.py data_range
 */
export interface DataRangeResponse {
    earliest: string | null;
    latest: string | null;
    count: number;
}

/**
 * 最終受信レスポンス（GET /api/last-received）
 * 対応: graph.py last_received
 */
export interface LastReceivedResponse {
    mode_s: string | null;
    vdl2: string | null;
}

/**
 * 受信方式毎の受信品質
 * 対応: data_routes.py receiver_quality
 */
export interface MethodQuality {
    last_hour: number;
    last_24h: number;
    last_received: string | null;
    age_seconds: number | null;
}

/**
 * 受信品質レスポンス（GET /api/receiver-quality）
 * 対応: data_routes.py receiver_quality
 */
export interface ReceiverQualityResponse {
    mode_s: MethodQuality;
    vdl2: MethodQuality;
    aggregates: Record<string, number>;
}

/**
 * 集約統計の各テーブル情報
 */
export interface AggregateTableStats {
    exists: boolean;
    stats: Record<string, unknown> | null;
}

/**
 * 集約統計レスポンス（GET /api/aggregate-stats）
 * 対応: graph.py aggregate_stats
 */
export interface AggregateStatsResponse {
    [tableName: string]: AggregateTableStats;
}
