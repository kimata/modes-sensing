/**
 * 日時フォーマットの共通ユーティリティ
 *
 * DateSelector / GraphDisplay / useUrlParams に重複していた
 * ローカル時刻のフォーマット処理を統合。
 */

/**
 * ローカル時刻を "YYYY-MM-DD<separator>HH:mm" 形式にフォーマット
 */
function formatDateTimeLocal(date: Date, separator: string): string {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    const hours = String(date.getHours()).padStart(2, "0");
    const minutes = String(date.getMinutes()).padStart(2, "0");
    return `${year}-${month}-${day}${separator}${hours}:${minutes}`;
}

/**
 * datetime-local 入力用フォーマット（例: "2026-01-07T12:00"）
 */
export function formatDateForInput(date: Date): string {
    return formatDateTimeLocal(date, "T");
}

/**
 * URL パラメータ用フォーマット（例: "2026-01-07T12:00"）
 */
export function formatDateForUrl(date: Date): string {
    return formatDateTimeLocal(date, "T");
}

/**
 * 画面表示用フォーマット（例: "2026-01-07 12:00"）
 */
export function formatDateForDisplay(date: Date): string {
    return formatDateTimeLocal(date, " ");
}
