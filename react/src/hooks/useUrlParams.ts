/**
 * URL パラメータと状態の同期を管理するカスタムフック
 *
 * - start/end: 日時範囲（ISO 8601 ローカル形式）
 * - limitAltitude: 高度制限フラグ
 *
 * デフォルト値の場合は URL パラメータを付けない
 */

interface ParsedUrlState {
    hasUrlParams: boolean;
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

    const startStr = params.get("start");
    const endStr = params.get("end");
    const limitAltitudeStr = params.get("limitAltitude");

    const start = parseDateFromUrl(startStr || "");
    const end = parseDateFromUrl(endStr || "");
    const limitAltitude = limitAltitudeStr === "true";

    // URL パラメータが1つでもあれば hasUrlParams = true
    const hasUrlParams = startStr !== null || endStr !== null || limitAltitudeStr !== null;

    return {
        hasUrlParams,
        start,
        end,
        limitAltitude,
    };
}

/**
 * 現在の状態がデフォルトかどうかを判定
 * デフォルト: 7日間、高度制限なし
 */
export function isDefaultState(
    start: Date,
    end: Date,
    limitAltitude: boolean,
    defaultDays: number = 7
): boolean {
    // 高度制限がある場合は非デフォルト
    if (limitAltitude) return false;

    // 期間が指定日数かどうか（±1時間の誤差を許容）
    const durationMs = end.getTime() - start.getTime();
    const expectedDurationMs = defaultDays * 24 * 60 * 60 * 1000;
    const tolerance = 60 * 60 * 1000; // 1時間

    if (Math.abs(durationMs - expectedDurationMs) > tolerance) return false;

    // 終了日時が現在に近いかどうか（±1時間の誤差を許容）
    const now = new Date();
    if (Math.abs(end.getTime() - now.getTime()) > tolerance) return false;

    return true;
}

/**
 * URL を更新（履歴を汚さない replaceState を使用）
 */
export function updateUrl(start: Date, end: Date, limitAltitude: boolean): void {
    console.log("[updateUrl] called:", {
        start: formatDateForUrl(start),
        end: formatDateForUrl(end),
        limitAltitude,
        isDefault: isDefaultState(start, end, limitAltitude),
    });

    // デフォルト状態ならパラメータなし URL に
    if (isDefaultState(start, end, limitAltitude)) {
        console.log("[updateUrl] isDefault=true, skipping URL update");
        // パラメータがある場合のみ更新
        if (window.location.search) {
            const newUrl = window.location.pathname + window.location.hash;
            window.history.replaceState(null, "", newUrl);
        }
        return;
    }

    // 非デフォルト状態ならパラメータを設定
    const params = new URLSearchParams();
    params.set("start", formatDateForUrl(start));
    params.set("end", formatDateForUrl(end));

    // limitAltitude は true の時のみ設定
    if (limitAltitude) {
        params.set("limitAltitude", "true");
    }

    const newUrl = `${window.location.pathname}?${params.toString()}${window.location.hash}`;
    console.log("[updateUrl] updating URL to:", newUrl);
    window.history.replaceState(null, "", newUrl);
}

/**
 * URL をデフォルト（パラメータなし）にリセット
 */
export function resetUrl(): void {
    const newUrl = window.location.pathname;
    window.history.replaceState(null, "", newUrl);
}
