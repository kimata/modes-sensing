import { useState, useEffect, useRef, useCallback } from "react";
import type {
    BatchJobStatusEntry,
    BatchStatusResponse,
    CreateJobsResponse,
    JobStatusValue,
    SseStatusEventData,
} from "../types/api";

export interface GraphJob {
    jobId: string;
    graphName: string;
    status: JobStatusValue;
    progress: number;
    error: string | null;
    resultUrl: string | null;
    elapsedSeconds: number | null;
    stage: string | null;
    startTime: number | null; // ジョブ開始時刻（Date.now()）
    retryCount: number; // 失敗（failed/timeout）による自動リトライ回数
    unknownRetryCount: number; // unknown（ジョブ消失）による自動再作成回数
    isRetrying: boolean; // 自動リトライ中かどうか
}

interface UseGraphJobsOptions {
    dateRange: { start: Date; end: Date };
    limitAltitude: boolean;
    graphs: string[];
    pollingInterval?: number; // デフォルト: 1000ms
    enabled?: boolean; // デフォルト: true
}

interface UseGraphJobsResult {
    jobs: Record<string, GraphJob>;
    isLoading: boolean;
    reloadJob: (graphName: string) => Promise<void>;
    reloadAll: () => Promise<void>;
}

// ジョブ再作成のモード
type JobCreateMode = "manual" | "auto-failure" | "auto-unknown";

const API_BASE = "/modes-sensing/api";

// ポーリング連続失敗の上限（この回数失敗したらポーリングを停止しエラー表示）
const MAX_POLLING_FAILURES = 10;

// ポーリング指数バックオフの上限（ミリ秒）
const MAX_POLLING_BACKOFF_MS = 30000;

// 終端ステータス（これ以上変化しない）
const TERMINAL_STATUSES: readonly JobStatusValue[] = ["completed", "failed", "timeout"];

function buildInitialJob(graphName: string, jobId: string): GraphJob {
    return {
        jobId,
        graphName,
        status: "pending",
        progress: 0,
        error: null,
        resultUrl: null,
        elapsedSeconds: null,
        stage: null,
        startTime: Date.now(),
        retryCount: 0,
        unknownRetryCount: 0,
        isRetrying: false,
    };
}

export function useGraphJobs(options: UseGraphJobsOptions): UseGraphJobsResult {
    const { dateRange, limitAltitude, graphs, pollingInterval = 1000, enabled = true } = options;

    const [jobs, setJobs] = useState<Record<string, GraphJob>>({});
    const [isLoading, setIsLoading] = useState(false);

    // 最新のジョブ状態（setState updater 内で副作用を持たないためのミラー）
    const jobsRef = useRef<Record<string, GraphJob>>({});
    // 監視対象（未終端）のジョブ ID
    const activeJobIdsRef = useRef<string[]>([]);
    const eventSourceRef = useRef<EventSource | null>(null);
    const pollTimerRef = useRef<number | null>(null);
    const pollFailureCountRef = useRef(0);
    // SSE が失敗した場合（プロキシ非対応等）、セッション中はポーリングにフォールバック
    const sseDisabledRef = useRef(false);
    const mountedRef = useRef(true);
    // ジョブ一括作成の世代番号（古い非同期応答が新しい状態を上書きするのを防ぐ）
    const generationRef = useRef(0);

    // ジョブ作成リクエストに使う最新オプション（stale closure 対策）
    const requestOptionsRef = useRef({ dateRange, limitAltitude, graphs });
    requestOptionsRef.current = { dateRange, limitAltitude, graphs };

    // 状態を ref とステートの両方に反映する（updater 副作用を避けるため値渡し）
    const commitJobs = (next: Record<string, GraphJob>) => {
        jobsRef.current = next;
        setJobs(next);
    };

    // ---- 以下の関数群は相互再帰するため、hoisting の効く function 宣言で定義する。
    //      可変データはすべて ref 経由で参照するので、古いレンダーのクロージャでも安全。

    // SSE 接続とポーリングタイマーを停止
    function stopWatching(): void {
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
            eventSourceRef.current = null;
        }
        if (pollTimerRef.current !== null) {
            clearTimeout(pollTimerRef.current);
            pollTimerRef.current = null;
        }
    }

    // 処理中の全ジョブをエラー状態にする（ポーリング連続失敗時）
    function markActiveJobsFailed(errorMessage: string): void {
        const prev = jobsRef.current;
        const updated = { ...prev };
        let changed = false;

        Object.keys(updated).forEach((graphName) => {
            const job = updated[graphName];
            if (job.status === "pending" || job.status === "processing") {
                updated[graphName] = {
                    ...job,
                    status: "failed",
                    error: errorMessage,
                    isRetrying: false,
                };
                changed = true;
            }
        });

        if (changed) {
            commitJobs(updated);
        }
        activeJobIdsRef.current = [];
    }

    // ステータス応答（ポーリング / SSE 共通）を反映する
    function handleStatusData(entries: Record<string, BatchJobStatusEntry>): void {
        const prev = jobsRef.current;
        const updated = { ...prev };
        let changed = false;
        const finishedJobIds: string[] = [];
        const failureRetryTargets: string[] = [];
        const unknownRecreateTargets: string[] = [];

        Object.entries(entries).forEach(([jobId, entry]) => {
            const graphName = Object.keys(prev).find((key) => prev[key].jobId === jobId);
            if (!graphName) return;
            const job = updated[graphName];
            // 古い jobId のステータスは無視（リトライで置き換わった場合）
            if (job.jobId !== jobId) return;

            if (entry.status === "unknown") {
                // ジョブがサーバー上で消失（再起動等）。初回のみ自動再作成する
                finishedJobIds.push(jobId);
                if (job.unknownRetryCount === 0) {
                    unknownRecreateTargets.push(graphName);
                    updated[graphName] = {
                        ...job,
                        status: "pending",
                        progress: 0,
                        error: null,
                        resultUrl: null,
                        isRetrying: true,
                    };
                } else {
                    updated[graphName] = {
                        ...job,
                        status: "failed",
                        error: "ジョブがサーバー上で見つかりませんでした",
                        resultUrl: null,
                        isRetrying: false,
                    };
                }
                changed = true;
                return;
            }

            const isTerminal = TERMINAL_STATUSES.includes(entry.status);
            const isFailed = entry.status === "failed" || entry.status === "timeout";

            if (isTerminal) {
                finishedJobIds.push(jobId);
            }

            // 失敗かつ未リトライの場合、自動リトライ（リトライ中表示にする）
            if (isFailed && job.retryCount === 0) {
                failureRetryTargets.push(graphName);
                updated[graphName] = {
                    ...job,
                    status: "pending",
                    progress: 0,
                    error: null,
                    resultUrl: null,
                    isRetrying: true,
                };
                changed = true;
                return;
            }

            updated[graphName] = {
                ...job,
                status: entry.status,
                progress: entry.progress,
                error: entry.error,
                elapsedSeconds: entry.elapsed_seconds,
                stage: entry.stage || null,
                // ステータスが取得できた時点でリトライ中表示は解除する
                isRetrying: false,
                resultUrl: entry.status === "completed" ? `${API_BASE}/graph/job/${jobId}/result` : null,
            };
            changed = true;
        });

        if (finishedJobIds.length > 0) {
            activeJobIdsRef.current = activeJobIdsRef.current.filter((id) => !finishedJobIds.includes(id));
        }

        if (changed) {
            commitJobs(updated);
        }

        // 自動リトライ・再作成を実行（新しい jobId で監視を張り直す）
        failureRetryTargets.forEach((graphName) => {
            void createSingleJob(graphName, "auto-failure");
        });
        unknownRecreateTargets.forEach((graphName) => {
            void createSingleJob(graphName, "auto-unknown");
        });

        // 全ジョブが終端に達し、リトライ対象もなければ監視を停止
        if (
            activeJobIdsRef.current.length === 0 &&
            failureRetryTargets.length === 0 &&
            unknownRecreateTargets.length === 0
        ) {
            stopWatching();
            setIsLoading(false);
        }
    }

    // ステータスをポーリング（指数バックオフ付き）
    async function pollStatus(): Promise<void> {
        pollTimerRef.current = null;
        if (!mountedRef.current) return;

        const generation = generationRef.current;
        const ids = activeJobIdsRef.current;
        if (ids.length === 0) {
            setIsLoading(false);
            return;
        }

        try {
            const response = await fetch(
                `${API_BASE}/graph/jobs/status?job_ids=${encodeURIComponent(ids.join(","))}`
            );

            if (!response.ok) {
                throw new Error(
                    response.status === 502
                        ? "サーバーが一時的に利用できません"
                        : `サーバーエラー (${response.status})`
                );
            }

            const data: BatchStatusResponse = await response.json();
            if (!mountedRef.current || generation !== generationRef.current) return;

            pollFailureCountRef.current = 0;
            handleStatusData(data.jobs);

            // handleStatusData で監視が継続している場合のみ次回をスケジュール
            if (activeJobIdsRef.current.length > 0 && eventSourceRef.current === null) {
                schedulePoll(pollingInterval);
            }
        } catch (error) {
            console.error("Failed to poll status:", error);
            if (!mountedRef.current || generation !== generationRef.current) return;

            pollFailureCountRef.current += 1;
            const failures = pollFailureCountRef.current;

            if (failures >= MAX_POLLING_FAILURES) {
                // 連続失敗の上限に達したらポーリングを停止し、エラー表示（再試行ボタンで再開可能）
                const message =
                    error instanceof Error && error.message ? error.message : "サーバーに接続できません";
                stopWatching();
                markActiveJobsFailed(message);
                setIsLoading(false);
            } else {
                // 指数バックオフ: 1s → 2s → 4s → ... 最大 30s
                const backoffMs = Math.min(pollingInterval * 2 ** (failures - 1), MAX_POLLING_BACKOFF_MS);
                schedulePoll(backoffMs);
            }
        }
    }

    function schedulePoll(delayMs: number): void {
        if (pollTimerRef.current !== null) {
            clearTimeout(pollTimerRef.current);
        }
        pollTimerRef.current = window.setTimeout(() => {
            void pollStatus();
        }, delayMs);
    }

    // SSE（Server-Sent Events）で進捗を購読する
    function startSse(jobIds: string[]): void {
        const url = `${API_BASE}/graph/job/events?job_ids=${encodeURIComponent(jobIds.join(","))}`;
        const es = new EventSource(url);
        eventSourceRef.current = es;

        es.addEventListener("status", (event: Event) => {
            if (!mountedRef.current) return;
            try {
                const data: SseStatusEventData = JSON.parse((event as MessageEvent).data as string);
                pollFailureCountRef.current = 0;
                handleStatusData(data.jobs);
            } catch (error) {
                console.error("Failed to parse SSE status event:", error);
            }
        });

        es.addEventListener("done", () => {
            es.close();
            if (eventSourceRef.current === es) {
                eventSourceRef.current = null;
            }
            if (!mountedRef.current) return;
            // 全ジョブ終端でサーバーがストリームを閉じた。
            // 万一未終端のジョブが残っている場合はポーリングで補完する
            if (activeJobIdsRef.current.length > 0) {
                schedulePoll(0);
            } else {
                setIsLoading(false);
            }
        });

        es.onerror = () => {
            // プロキシ非対応・接続断等。SSE を諦めて改善済みポーリングにフォールバック
            es.close();
            if (eventSourceRef.current !== es) return;
            eventSourceRef.current = null;
            if (!mountedRef.current) return;

            sseDisabledRef.current = true;
            if (activeJobIdsRef.current.length > 0) {
                schedulePoll(0);
            }
        };
    }

    // 現在の activeJobIds に対する監視（SSE またはポーリング）を張り直す
    function startWatching(): void {
        stopWatching();

        const ids = activeJobIdsRef.current;
        if (ids.length === 0) {
            setIsLoading(false);
            return;
        }

        if (typeof EventSource !== "undefined" && !sseDisabledRef.current) {
            startSse(ids);
        } else {
            schedulePoll(0);
        }
    }

    // 単一ジョブを作成（手動リロード / 自動リトライ / unknown 再作成）
    async function createSingleJob(graphName: string, mode: JobCreateMode): Promise<void> {
        const isAuto = mode !== "manual";
        const generation = generationRef.current;
        const { dateRange: range, limitAltitude: limited } = requestOptionsRef.current;

        // 先にローディング / リトライ中表示へ切り替える
        {
            const prev = jobsRef.current;
            const prevJob = prev[graphName] ?? buildInitialJob(graphName, "");
            commitJobs({
                ...prev,
                [graphName]: {
                    ...prevJob,
                    status: "pending",
                    progress: 0,
                    error: null,
                    resultUrl: null,
                    elapsedSeconds: null,
                    stage: null,
                    isRetrying: isAuto,
                },
            });
        }

        try {
            const response = await fetch(`${API_BASE}/graph/job`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    graphs: [graphName],
                    start: range.start.toISOString(),
                    end: range.end.toISOString(),
                    limit_altitude: limited,
                }),
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data: CreateJobsResponse = await response.json();
            if (!mountedRef.current || generation !== generationRef.current) return;

            if (data.jobs.length === 0) {
                throw new Error("No job created");
            }

            const newJob = data.jobs[0];
            const prev = jobsRef.current;
            const prevJob = prev[graphName];
            const oldJobId = prevJob?.jobId;

            commitJobs({
                ...prev,
                [graphName]: {
                    ...buildInitialJob(graphName, newJob.job_id),
                    retryCount:
                        mode === "auto-failure"
                            ? (prevJob?.retryCount ?? 0) + 1
                            : mode === "manual"
                              ? 0
                              : (prevJob?.retryCount ?? 0),
                    unknownRetryCount:
                        mode === "auto-unknown"
                            ? (prevJob?.unknownRetryCount ?? 0) + 1
                            : mode === "manual"
                              ? 0
                              : (prevJob?.unknownRetryCount ?? 0),
                    isRetrying: isAuto,
                },
            });

            // 古い jobId を除外し、新しい jobId を監視対象に追加
            activeJobIdsRef.current = [
                ...activeJobIdsRef.current.filter((id) => id !== oldJobId),
                newJob.job_id,
            ];
            pollFailureCountRef.current = 0;
            setIsLoading(true);

            // SSE は新しい job_ids で張り直す（ポーリングも同様に再開）
            startWatching();
        } catch (error) {
            console.error("Failed to create job:", error);
            if (!mountedRef.current || generation !== generationRef.current) return;

            // POST 失敗時は該当グラフをエラー状態にする（永久スピナーの防止）
            const prev = jobsRef.current;
            const prevJob = prev[graphName] ?? buildInitialJob(graphName, "");
            commitJobs({
                ...prev,
                [graphName]: {
                    ...prevJob,
                    status: "failed",
                    error: "ジョブの作成に失敗しました",
                    isRetrying: false,
                },
            });
        }
    }

    // 全グラフのジョブを作成
    async function createJobs(): Promise<void> {
        const { dateRange: range, limitAltitude: limited, graphs: graphNames } = requestOptionsRef.current;

        if (graphNames.length === 0) return;

        const generation = ++generationRef.current;
        setIsLoading(true);
        stopWatching();
        pollFailureCountRef.current = 0;

        try {
            const response = await fetch(`${API_BASE}/graph/job`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    graphs: graphNames,
                    start: range.start.toISOString(),
                    end: range.end.toISOString(),
                    limit_altitude: limited,
                }),
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data: CreateJobsResponse = await response.json();
            if (!mountedRef.current || generation !== generationRef.current) return;

            const initialJobs: Record<string, GraphJob> = {};
            const newJobIds: string[] = [];

            data.jobs.forEach((job) => {
                initialJobs[job.graph_name] = buildInitialJob(job.graph_name, job.job_id);
                newJobIds.push(job.job_id);
            });

            commitJobs(initialJobs);
            activeJobIdsRef.current = newJobIds;

            startWatching();
        } catch (error) {
            console.error("Failed to create jobs:", error);
            if (!mountedRef.current || generation !== generationRef.current) return;

            // POST 失敗時は全グラフをエラー状態にする（0% スピナーのまま放置しない）
            const failedJobs: Record<string, GraphJob> = {};
            graphNames.forEach((graphName) => {
                failedJobs[graphName] = {
                    ...buildInitialJob(graphName, ""),
                    status: "failed",
                    error: "ジョブの作成に失敗しました",
                };
            });
            commitJobs(failedJobs);
            activeJobIdsRef.current = [];
            setIsLoading(false);
        }
    }

    // 単一ジョブをリロード（エラーカードの再試行ボタン等から呼ばれる）
    const reloadJob = useCallback(async (graphName: string) => {
        await createSingleJob(graphName, "manual");
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // 全ジョブをリロード
    const reloadAll = useCallback(async () => {
        await createJobs();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // graphs配列の変更を検出（参照ではなく内容で比較）
    const graphsKey = graphs.join(",");

    // 日付範囲、設定、グラフリストが変わったらジョブを再作成
    // 単一のuseEffectで全ての依存関係を管理（重複呼び出しを防止）
    useEffect(() => {
        mountedRef.current = true;

        if (enabled) {
            void createJobs();
        }

        return () => {
            mountedRef.current = false;
            stopWatching();
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [dateRange.start.getTime(), dateRange.end.getTime(), limitAltitude, enabled, graphsKey]);

    return {
        jobs,
        isLoading,
        reloadJob,
        reloadAll,
    };
}
