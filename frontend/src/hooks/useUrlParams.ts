/**
 * URL パラメータと状態の同期を管理
 *
 * クイック選択: ?period=30days&limitAltitude=true
 * カスタム: ?start=2025-08-24T23:22&end=2026-01-07T14:20&limitAltitude=true
 *
 * デフォルト（7日間、高度制限なし）ではパラメータを付けない
 */

// 期間タイプの定義
export type PeriodType = "1day" | "7days" | "30days" | "180days" | "365days" | "custom";

// 期間タイプと日数のマッピング
export const PERIOD_DAYS: Record<Exclude<PeriodType, "custom">, number> = {
    "1day": 1,
    "7days": 7,
    "30days": 30,
    "180days": 180,
    "365days": 365,
};

interface ParsedUrlState {
    hasUrlParams: boolean;
    period: PeriodType | null;
    start: Date | null;
    end: Date | null;
    limitAltitude: boolean;
}

/**
 * 日時を URL パラメータ用の文字列に変換（ローカル時間、秒なし）
 * 例: "2026-01-07T12:00"
 */
export function formatDateForUrl(date: Date): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const hours = String(date.getHours()).padStart(2, "0");
    const minutes = String(date.getMinutes()).padStart(2, "0");
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

/**
 * URL パラメータ文字列から日時を解析
 */
function parseDateFromUrl(str: string): Date | null {
    if (!str) return null;

    // ISO 8601 形式をパース（ローカル時間として解釈）
    const match = str.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})$/);
    if (!match) return null;

    const [, year, month, day, hours, minutes] = match;
    const date = new Date(
        parseInt(year),
        parseInt(month) - 1,
        parseInt(day),
        parseInt(hours),
        parseInt(minutes),
        0,
        0
    );

    // 有効な日付かチェック
    if (isNaN(date.getTime())) return null;

    return date;
}

/**
 * 現在の URL から パラメータを解析
 */
export function parseUrlParams(): ParsedUrlState {
    const params = new URLSearchParams(window.location.search);

    const periodStr = params.get("period");
    const startStr = params.get("start");
    const endStr = params.get("end");
    const limitAltitudeStr = params.get("limitAltitude");

    // period パラメータがある場合
    if (periodStr && periodStr in PERIOD_DAYS) {
        return {
            hasUrlParams: true,
            period: periodStr as PeriodType,
            start: null,
            end: null,
            limitAltitude: limitAltitudeStr === "true",
        };
    }

    // start/end パラメータがある場合（カスタム）
    const start = parseDateFromUrl(startStr || "");
    const end = parseDateFromUrl(endStr || "");

    if (start && end) {
        return {
            hasUrlParams: true,
            period: "custom",
            start,
            end,
            limitAltitude: limitAltitudeStr === "true",
        };
    }

    // limitAltitude のみの場合
    if (limitAltitudeStr !== null) {
        return {
            hasUrlParams: true,
            period: null,
            start: null,
            end: null,
            limitAltitude: limitAltitudeStr === "true",
        };
    }

    // パラメータなし
    return {
        hasUrlParams: false,
        period: null,
        start: null,
        end: null,
        limitAltitude: false,
    };
}

/**
 * URL を更新（履歴を汚さない replaceState を使用）
 *
 * @param period - 期間タイプ（クイック選択 or カスタム）
 * @param start - 開始日時（カスタム時のみ使用）
 * @param end - 終了日時（カスタム時のみ使用）
 * @param limitAltitude - 高度制限
 */
export function updateUrl(period: PeriodType, start: Date, end: Date, limitAltitude: boolean): void {
    // デフォルト状態（7日間、高度制限なし）ならパラメータなし
    if (period === "7days" && !limitAltitude) {
        if (window.location.search) {
            const newUrl = window.location.pathname + window.location.hash;
            window.history.replaceState(null, "", newUrl);
        }
        return;
    }

    const params = new URLSearchParams();

    if (period === "custom") {
        // カスタム期間: start/end パラメータ
        params.set("start", formatDateForUrl(start));
        params.set("end", formatDateForUrl(end));
    } else {
        // クイック選択: period パラメータ
        params.set("period", period);
    }

    // 高度制限
    if (limitAltitude) {
        params.set("limitAltitude", "true");
    }

    const newUrl = `${window.location.pathname}?${params.toString()}${window.location.hash}`;
    window.history.replaceState(null, "", newUrl);
}

/**
 * URL をデフォルト（パラメータなし）にリセット
 */
export function resetUrl(): void {
    const newUrl = window.location.pathname;
    window.history.replaceState(null, "", newUrl);
}
