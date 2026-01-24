// API 型定義を再エクスポート
export * from "./api";

// 後方互換性のためのエイリアス
// 新しいコードでは LastReceivedResponse を使用してください
export type { LastReceivedResponse as LastReceived } from "./api";

export interface SysInfo {
    date: string;
    timezone: string;
    image_build_date: string;
    uptime: string;
    load_average: string;
    cpu_usage: number;
    memory_usage_percent: number;
    memory_free_mb: number;
    disk_usage_percent: number;
    disk_free_mb: number;
    process_count: number;
    cpu_temperature?: number;
}
