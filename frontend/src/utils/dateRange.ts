/**
 * データ範囲へのクランプ + 期間再計算の共通ロジック
 *
 * DateSelector（クイック選択・カスタムボタン・カスタム確定）と
 * App（初期日付範囲の調整）に重複していた処理を統合。
 */

import type { DataRangeResponse } from "../types/api";

export interface ClampOptions {
    /**
     * 指定した場合、クランプ後の終了日時から逆算してこの期間（ミリ秒）を
     * 維持するよう開始日時を再計算する（データ最古日時を下回らない範囲で）。
     */
    preservePeriodMs?: number;
    /**
     * クランプの結果、開始 > 終了となり期間が成立しない場合に
     * alert でユーザーへ通知する。
     */
    showAlert?: boolean;
}

export interface ClampResult {
    start: Date;
    end: Date;
    /** データ範囲による調整が行われたかどうか */
    adjusted: boolean;
    /** クランプ後の期間が有効（start <= end）かどうか */
    valid: boolean;
}

/**
 * 日時範囲を利用可能なデータ範囲内にクランプする
 *
 * - 終了日時がデータ最新日時を超える場合はデータ最新日時に調整
 * - 開始日時がデータ最古日時を下回る場合はデータ最古日時に調整
 * - preservePeriodMs 指定時は終了日時から逆算して期間の維持を試みる
 */
export function clampRangeToData(
    start: Date,
    end: Date,
    dataRange: DataRangeResponse | null,
    options: ClampOptions = {}
): ClampResult {
    let clampedStart = new Date(start);
    let clampedEnd = new Date(end);
    clampedStart.setSeconds(0, 0);
    clampedEnd.setSeconds(0, 0);
    let adjusted = false;

    if (!dataRange || !dataRange.earliest || !dataRange.latest) {
        return { start: clampedStart, end: clampedEnd, adjusted, valid: clampedStart <= clampedEnd };
    }

    const dataEarliest = new Date(dataRange.earliest);
    const dataLatest = new Date(dataRange.latest);

    // 終了日時が利用可能なデータの最新日時を超えている場合
    if (clampedEnd > dataLatest) {
        clampedEnd = new Date(dataLatest);
        clampedEnd.setSeconds(0, 0);
        adjusted = true;
    }

    // 開始日時が利用可能なデータの最古日時を下回っている場合
    if (clampedStart < dataEarliest) {
        clampedStart = new Date(dataEarliest);
        clampedStart.setSeconds(0, 0);
        adjusted = true;
    }

    // 指定期間の維持を試みる（終了日時から逆算）
    if (options.preservePeriodMs !== undefined) {
        const recalculatedStart = new Date(clampedEnd.getTime() - options.preservePeriodMs);
        if (recalculatedStart >= dataEarliest) {
            recalculatedStart.setSeconds(0, 0);
            if (recalculatedStart.getTime() !== clampedStart.getTime()) {
                adjusted = true;
            }
            clampedStart = recalculatedStart;
        }
    }

    const valid = clampedStart <= clampedEnd;

    if (!valid && options.showAlert) {
        const formatJa = (d: Date) =>
            d.toLocaleDateString("ja-JP") +
            " " +
            d.toLocaleTimeString("ja-JP", { hour: "2-digit", minute: "2-digit" });
        alert(
            `利用可能な期間（${formatJa(dataEarliest)} ～ ${formatJa(dataLatest)}）では、` +
                "指定された期間を設定できません"
        );
    }

    return { start: clampedStart, end: clampedEnd, adjusted, valid };
}
