import { useState, useEffect, useRef, useCallback } from 'react'

// 型定義
interface JobInfo {
  job_id: string
  graph_name: string
}

interface CreateJobsResponse {
  jobs: JobInfo[]
}

interface JobStatusInfo {
  status: 'pending' | 'processing' | 'completed' | 'failed' | 'timeout'
  progress: number
  graph_name: string
  error: string | null
  elapsed_seconds: number | null
}

interface BatchStatusResponse {
  jobs: Record<string, JobStatusInfo>
}

export interface GraphJob {
  jobId: string
  graphName: string
  status: 'pending' | 'processing' | 'completed' | 'failed' | 'timeout'
  progress: number
  error: string | null
  resultUrl: string | null
}

interface UseGraphJobsOptions {
  dateRange: { start: Date; end: Date }
  limitAltitude: boolean
  graphs: string[]
  pollingInterval?: number  // デフォルト: 1000ms
  enabled?: boolean  // デフォルト: true
}

interface UseGraphJobsResult {
  jobs: Record<string, GraphJob>
  isLoading: boolean
  reloadJob: (graphName: string) => Promise<void>
  reloadAll: () => Promise<void>
}

export function useGraphJobs(options: UseGraphJobsOptions): UseGraphJobsResult {
  const {
    dateRange,
    limitAltitude,
    graphs,
    pollingInterval = 1000,
    enabled = true
  } = options

  const [jobs, setJobs] = useState<Record<string, GraphJob>>({})
  const [isLoading, setIsLoading] = useState(false)
  const pollingRef = useRef<number | null>(null)
  const jobIdsRef = useRef<string[]>([])
  const mountedRef = useRef(true)

  // ポーリングを停止
  const stopPolling = useCallback(() => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current)
      pollingRef.current = null
    }
  }, [])

  // ステータスをポーリング
  const pollStatus = useCallback(async () => {
    if (jobIdsRef.current.length === 0) return

    try {
      const response = await fetch('/modes-sensing/api/graph/jobs/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ job_ids: jobIdsRef.current })
      })

      if (!response.ok) {
        console.error('Failed to poll status:', response.status)
        return
      }

      const data: BatchStatusResponse = await response.json()

      if (!mountedRef.current) return

      setJobs(prev => {
        const updated = { ...prev }
        let allCompleted = true
        const completedJobIds: string[] = []

        Object.entries(data.jobs).forEach(([jobId, status]) => {
          const graphName = Object.keys(updated).find(
            key => updated[key].jobId === jobId
          )

          if (graphName) {
            const isCompleted = status.status === 'completed' ||
                               status.status === 'failed' ||
                               status.status === 'timeout'

            updated[graphName] = {
              ...updated[graphName],
              status: status.status,
              progress: status.progress,
              error: status.error,
              resultUrl: status.status === 'completed'
                ? `/modes-sensing/api/graph/job/${jobId}/result`
                : null
            }

            if (isCompleted) {
              completedJobIds.push(jobId)
            } else {
              allCompleted = false
            }
          }
        })

        // 完了したジョブをポーリング対象から除外
        if (completedJobIds.length > 0) {
          jobIdsRef.current = jobIdsRef.current.filter(
            id => !completedJobIds.includes(id)
          )
        }

        // 全て完了したらポーリング停止
        if (allCompleted) {
          stopPolling()
          setIsLoading(false)
        }

        return updated
      })

    } catch (error) {
      console.error('Failed to poll status:', error)
    }
  }, [stopPolling])

  // ポーリングを開始
  const startPolling = useCallback(() => {
    // 既存のポーリングを停止
    stopPolling()

    // 初回は即座に実行
    pollStatus()

    // 定期的にポーリング
    pollingRef.current = window.setInterval(pollStatus, pollingInterval)
  }, [pollStatus, pollingInterval, stopPolling])

  // ジョブを作成
  const createJobs = useCallback(async () => {
    if (!enabled || graphs.length === 0) return

    setIsLoading(true)
    stopPolling()

    try {
      const response = await fetch('/modes-sensing/api/graph/job', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          graphs,
          start: dateRange.start.toISOString(),
          end: dateRange.end.toISOString(),
          limit_altitude: limitAltitude
        })
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data: CreateJobsResponse = await response.json()

      if (!mountedRef.current) return

      // 初期状態を設定
      const initialJobs: Record<string, GraphJob> = {}
      const newJobIds: string[] = []

      data.jobs.forEach(job => {
        initialJobs[job.graph_name] = {
          jobId: job.job_id,
          graphName: job.graph_name,
          status: 'pending',
          progress: 0,
          error: null,
          resultUrl: null
        }
        newJobIds.push(job.job_id)
      })

      setJobs(initialJobs)
      jobIdsRef.current = newJobIds

      // ポーリング開始
      startPolling()

    } catch (error) {
      console.error('Failed to create jobs:', error)
      setIsLoading(false)
    }
  }, [dateRange, limitAltitude, graphs, enabled, stopPolling, startPolling])

  // 単一ジョブをリロード
  const reloadJob = useCallback(async (graphName: string) => {
    try {
      const response = await fetch('/modes-sensing/api/graph/job', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          graphs: [graphName],
          start: dateRange.start.toISOString(),
          end: dateRange.end.toISOString(),
          limit_altitude: limitAltitude
        })
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data: CreateJobsResponse = await response.json()

      if (data.jobs.length > 0) {
        const newJob = data.jobs[0]

        setJobs(prev => ({
          ...prev,
          [graphName]: {
            jobId: newJob.job_id,
            graphName,
            status: 'pending',
            progress: 0,
            error: null,
            resultUrl: null
          }
        }))

        // ポーリング対象に追加
        jobIdsRef.current = [...jobIdsRef.current, newJob.job_id]

        // ポーリングが停止していたら再開
        if (!pollingRef.current) {
          startPolling()
          setIsLoading(true)
        }
      }
    } catch (error) {
      console.error('Failed to reload job:', error)
    }
  }, [dateRange, limitAltitude, startPolling])

  // 全ジョブをリロード
  const reloadAll = useCallback(async () => {
    await createJobs()
  }, [createJobs])

  // graphs配列の変更を検出（参照ではなく内容で比較）
  const graphsKey = graphs.join(',')

  // 日付範囲、設定、グラフリストが変わったらジョブを再作成
  // 単一のuseEffectで全ての依存関係を管理（重複呼び出しを防止）
  useEffect(() => {
    mountedRef.current = true

    if (enabled) {
      createJobs()
    }

    return () => {
      mountedRef.current = false
      stopPolling()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dateRange.start.getTime(), dateRange.end.getTime(), limitAltitude, enabled, graphsKey])

  return {
    jobs,
    isLoading,
    reloadJob,
    reloadAll
  }
}
