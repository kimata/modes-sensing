import { useState, useEffect, useRef, useCallback } from "react";
import type { CreateJobsResponse, BatchStatusResponse, JobStatusValue } from "../types/api";

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
    retryCount: number; // リトライ回数（0: 未リトライ、1: 1回リトライ済み）
    isRetrying: boolean; // 自動リトライ中かどうか
    pollingFailureCount: number; // ポーリング連続失敗回数
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

// ポーリング失敗の閾値（この回数連続失敗で自動リトライ）
const POLLING_FAILURE_THRESHOLD = 5;

export function useGraphJobs(options: UseGraphJobsOptions): UseGraphJobsResult {
    const { dateRange, limitAltitude, graphs, pollingInterval = 1000, enabled = true } = options;

    const [jobs, setJobs] = useState<Record<string, GraphJob>>({});
    const [isLoading, setIsLoading] = useState(false);
    const pollingRef = useRef<number | null>(null);
    const jobIdsRef = useRef<string[]>([]);
    const mountedRef = useRef(true);
    const pendingRetriesRef = useRef<string[]>([]); // 自動リトライ待ちのグラフ名

    // ポーリングを停止
    const stopPolling = useCallback(() => {
        if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
        }
    }, []);

    // ポーリング失敗時の処理（502エラー等）
    const handlePollingFailure = useCallback((errorMessage: string) => {
        if (!mountedRef.current) return;

        setJobs((prev) => {
            const updated = { ...prev };
            const retryTargets: string[] = [];

            // 処理中のジョブの失敗カウントをインクリメント
            Object.keys(updated).forEach((graphName) => {
                const job = updated[graphName];
                if (job.status === "pending" || job.status === "processing") {
                    const newFailureCount = job.pollingFailureCount + 1;
                    updated[graphName] = {
                        ...job,
                        pollingFailureCount: newFailureCount,
                    };

                    // 閾値を超えた場合、自動リトライ対象に追加（未リトライの場合のみ）
                    if (newFailureCount >= POLLING_FAILURE_THRESHOLD && job.retryCount === 0) {
                        retryTargets.push(graphName);
                        updated[graphName] = {
                            ...updated[graphName],
                            status: "failed",
                            error: errorMessage,
                        };
                    }
                }
            });

            // 自動リトライ対象があれば登録
            if (retryTargets.length > 0) {
                pendingRetriesRef.current = [...pendingRetriesRef.current, ...retryTargets];
            }

            return updated;
        });
    }, []);

    // ステータスをポーリング
    const pollStatus = useCallback(async () => {
        if (jobIdsRef.current.length === 0) return;

        try {
            const response = await fetch("/modes-sensing/api/graph/jobs/status", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ job_ids: jobIdsRef.current }),
            });

            if (!response.ok) {
                console.error("Failed to poll status:", response.status);
                // 502エラーなどの場合、ポーリング失敗として処理
                handlePollingFailure(
                    response.status === 502
                        ? "サーバーが一時的に利用できません"
                        : `サーバーエラー (${response.status})`
                );
                return;
            }

            const data: BatchStatusResponse = await response.json();

            if (!mountedRef.current) return;

            setJobs((prev) => {
                const updated = { ...prev };
                let allCompleted = true;
                const completedJobIds: string[] = [];
                const retryTargets: string[] = [];

                Object.entries(data.jobs).forEach(([jobId, status]) => {
                    const graphName = Object.keys(updated).find((key) => updated[key].jobId === jobId);

                    if (graphName) {
                        const isCompleted =
                            status.status === "completed" ||
                            status.status === "failed" ||
                            status.status === "timeout";
                        const isFailed = status.status === "failed" || status.status === "timeout";
                        const currentJob = updated[graphName];

                        // 失敗かつ未リトライの場合、自動リトライ対象に追加
                        if (isFailed && currentJob.retryCount === 0) {
                            retryTargets.push(graphName);
                        }

                        updated[graphName] = {
                            ...updated[graphName],
                            status: status.status,
                            progress: status.progress,
                            error: status.error,
                            elapsedSeconds: status.elapsed_seconds,
                            stage: status.stage || null,
                            pollingFailureCount: 0, // 成功したのでリセット
                            resultUrl:
                                status.status === "completed"
                                    ? `/modes-sensing/api/graph/job/${jobId}/result`
                                    : null,
                        };

                        if (isCompleted) {
                            completedJobIds.push(jobId);
                        } else {
                            allCompleted = false;
                        }
                    }
                });

                // 自動リトライ対象があれば登録
                if (retryTargets.length > 0) {
                    pendingRetriesRef.current = [...pendingRetriesRef.current, ...retryTargets];
                }

                // 完了したジョブをポーリング対象から除外
                if (completedJobIds.length > 0) {
                    jobIdsRef.current = jobIdsRef.current.filter((id) => !completedJobIds.includes(id));
                }

                // 全て完了したらポーリング停止（リトライ対象がない場合のみ）
                if (allCompleted && retryTargets.length === 0) {
                    stopPolling();
                    setIsLoading(false);
                }

                return updated;
            });
        } catch (error) {
            console.error("Failed to poll status:", error);
            // ネットワークエラーの場合もポーリング失敗として処理
            handlePollingFailure("ネットワークエラー");
        }
    }, [stopPolling, handlePollingFailure]);

    // ポーリングを開始
    const startPolling = useCallback(() => {
        // 既存のポーリングを停止
        stopPolling();

        // 初回は即座に実行
        pollStatus();

        // 定期的にポーリング
        pollingRef.current = window.setInterval(pollStatus, pollingInterval);
    }, [pollStatus, pollingInterval, stopPolling]);

    // ジョブを作成
    const createJobs = useCallback(async () => {
        if (!enabled || graphs.length === 0) return;

        setIsLoading(true);
        stopPolling();

        try {
            const response = await fetch("/modes-sensing/api/graph/job", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    graphs,
                    start: dateRange.start.toISOString(),
                    end: dateRange.end.toISOString(),
                    limit_altitude: limitAltitude,
                }),
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data: CreateJobsResponse = await response.json();

            if (!mountedRef.current) return;

            // 初期状態を設定
            const initialJobs: Record<string, GraphJob> = {};
            const newJobIds: string[] = [];
            const now = Date.now();

            data.jobs.forEach((job) => {
                initialJobs[job.graph_name] = {
                    jobId: job.job_id,
                    graphName: job.graph_name,
                    status: "pending",
                    progress: 0,
                    error: null,
                    resultUrl: null,
                    elapsedSeconds: null,
                    stage: null,
                    startTime: now,
                    retryCount: 0,
                    isRetrying: false,
                    pollingFailureCount: 0,
                };
                newJobIds.push(job.job_id);
            });

            setJobs(initialJobs);
            jobIdsRef.current = newJobIds;

            // ポーリング開始
            startPolling();
        } catch (error) {
            console.error("Failed to create jobs:", error);
            setIsLoading(false);
        }
    }, [dateRange, limitAltitude, graphs, enabled, stopPolling, startPolling]);

    // 単一ジョブをリロード（isAutoRetry: 自動リトライかどうか）
    const reloadJob = useCallback(
        async (graphName: string, isAutoRetry: boolean = false) => {
            // 自動リトライの場合、先にisRetryingフラグを設定
            if (isAutoRetry) {
                setJobs((prev) => ({
                    ...prev,
                    [graphName]: {
                        ...prev[graphName],
                        isRetrying: true,
                        status: "pending",
                        progress: 0,
                        error: null,
                    },
                }));
            }

            try {
                const response = await fetch("/modes-sensing/api/graph/job", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        graphs: [graphName],
                        start: dateRange.start.toISOString(),
                        end: dateRange.end.toISOString(),
                        limit_altitude: limitAltitude,
                    }),
                });

                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }

                const data: CreateJobsResponse = await response.json();

                if (data.jobs.length > 0) {
                    const newJob = data.jobs[0];

                    setJobs((prev) => {
                        const prevJob = prev[graphName];
                        return {
                            ...prev,
                            [graphName]: {
                                jobId: newJob.job_id,
                                graphName,
                                status: "pending",
                                progress: 0,
                                error: null,
                                resultUrl: null,
                                elapsedSeconds: null,
                                stage: null,
                                startTime: Date.now(),
                                retryCount: isAutoRetry ? (prevJob?.retryCount ?? 0) + 1 : 0,
                                isRetrying: isAutoRetry,
                                pollingFailureCount: 0,
                            },
                        };
                    });

                    // ポーリング対象に追加
                    jobIdsRef.current = [...jobIdsRef.current, newJob.job_id];

                    // ポーリングが停止していたら再開
                    if (!pollingRef.current) {
                        startPolling();
                        setIsLoading(true);
                    }
                }
            } catch (error) {
                console.error("Failed to reload job:", error);
                // リトライ自体が失敗した場合もエラー状態にする
                if (isAutoRetry) {
                    setJobs((prev) => ({
                        ...prev,
                        [graphName]: {
                            ...prev[graphName],
                            status: "failed",
                            error: "接続エラー",
                            isRetrying: false,
                        },
                    }));
                }
            }
        },
        [dateRange, limitAltitude, startPolling]
    );

    // 全ジョブをリロード
    const reloadAll = useCallback(async () => {
        await createJobs();
    }, [createJobs]);

    // 自動リトライを処理
    useEffect(() => {
        if (pendingRetriesRef.current.length === 0) return;

        const retryTargets = [...pendingRetriesRef.current];
        pendingRetriesRef.current = [];

        // 各リトライ対象を自動リトライ
        retryTargets.forEach((graphName) => {
            reloadJob(graphName, true);
        });
    }, [jobs, reloadJob]);

    // graphs配列の変更を検出（参照ではなく内容で比較）
    const graphsKey = graphs.join(",");

    // 日付範囲、設定、グラフリストが変わったらジョブを再作成
    // 単一のuseEffectで全ての依存関係を管理（重複呼び出しを防止）
    useEffect(() => {
        mountedRef.current = true;

        if (enabled) {
            createJobs();
        }

        return () => {
            mountedRef.current = false;
            stopPolling();
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
