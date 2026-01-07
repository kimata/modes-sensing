import { useRef, useLayoutEffect, useEffect, useState } from 'react'
import styles from './GraphDisplay.module.css'
import { useGraphJobs } from '../hooks/useGraphJobs'

interface GraphDisplayProps {
  dateRange: {
    start: Date
    end: Date
  }
  limitAltitude: boolean
  onImageClick: (imageUrl: string) => void
}

interface GraphInfo {
  name: string        // APIで使用するグラフ名
  endpoint: string    // 従来のエンドポイント（コンテナRef用）
  title: string
  size: [number, number]  // [width, height] in pixels
}

// graph.pyのGRAPH_DEF_MAPに対応
const graphs: GraphInfo[] = [
  { name: 'scatter_2d', endpoint: '/modes-sensing/api/graph/scatter_2d', title: '2D散布図', size: [2400, 1600] },
  { name: 'contour_2d', endpoint: '/modes-sensing/api/graph/contour_2d', title: '2D等高線プロット', size: [2400, 1600] },
  { name: 'density', endpoint: '/modes-sensing/api/graph/density', title: '密度プロット', size: [2400, 1600] },
  { name: 'heatmap', endpoint: '/modes-sensing/api/graph/heatmap', title: 'ヒートマップ', size: [2400, 1600] },
  { name: 'temperature', endpoint: '/modes-sensing/api/graph/temperature', title: '高度別温度時系列', size: [2400, 1600] },
  { name: 'wind_direction', endpoint: '/modes-sensing/api/graph/wind_direction', title: '風向・風速分布', size: [2400, 1600] },
  { name: 'scatter_3d', endpoint: '/modes-sensing/api/graph/scatter_3d', title: '3D散布図', size: [2800, 2800] },
  { name: 'contour_3d', endpoint: '/modes-sensing/api/graph/contour_3d', title: '3D等高線プロット', size: [2800, 2800] }
]

// グラフ名のリストを生成
const graphNames = graphs.map(g => g.name)

// コンテナ高さを計算するヘルパー関数
const calculateActualHeight = (graph: GraphInfo, containerWidth: number): number => {
  const [imageWidth, imageHeight] = graph.size
  const aspectRatio = imageHeight / imageWidth
  return containerWidth * aspectRatio
}

const GraphDisplay: React.FC<GraphDisplayProps> = ({ dateRange, limitAltitude, onImageClick }) => {
  // 非同期ジョブフックを使用
  const { jobs, reloadJob } = useGraphJobs({
    dateRange,
    limitAltitude,
    graphs: graphNames,
    pollingInterval: 1000
  })

  // コンテナ幅の追跡
  const containerRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const [containerWidths, setContainerWidths] = useState<Record<string, number>>({})

  // 通知用ref
  const notificationRef = useRef<HTMLDivElement>(null)

  // コンテナ幅を測定
  const measureContainerWidths = () => {
    const newWidths: Record<string, number> = {}
    graphs.forEach(graph => {
      const container = containerRefs.current[graph.endpoint]
      if (container) {
        const rect = container.getBoundingClientRect()
        newWidths[graph.endpoint] = rect.width
      }
    })
    setContainerWidths(newWidths)
  }

  // 初回マウント時にコンテナ幅を測定
  useLayoutEffect(() => {
    setTimeout(measureContainerWidths, 100)
  }, [])

  // リサイズ時にコンテナ幅を再測定
  useEffect(() => {
    window.addEventListener('resize', measureContainerWidths)
    return () => window.removeEventListener('resize', measureContainerWidths)
  }, [])

  // パーマリンクコピー関連
  const showCopyNotification = (message: string) => {
    if (!notificationRef.current) return
    notificationRef.current.textContent = message
    notificationRef.current.classList.add(styles.show)
    setTimeout(() => {
      notificationRef.current?.classList.remove(styles.show)
    }, 3000)
  }

  const copyPermalink = (elementId: string) => {
    const permalink = window.location.origin + window.location.pathname + '#' + elementId

    if (navigator.clipboard?.writeText) {
      navigator.clipboard.writeText(permalink)
        .then(() => {
          showCopyNotification('パーマリンクをコピーしました')
          window.history.pushState(null, '', '#' + elementId)
        })
        .catch(() => fallbackCopyToClipboard(permalink, elementId))
    } else {
      fallbackCopyToClipboard(permalink, elementId)
    }
  }

  const fallbackCopyToClipboard = (text: string, elementId: string) => {
    const textArea = document.createElement('textarea')
    textArea.value = text
    textArea.style.cssText = 'position:fixed;left:-9999px'
    document.body.appendChild(textArea)
    textArea.select()

    try {
      if (document.execCommand('copy')) {
        showCopyNotification('パーマリンクをコピーしました')
        window.history.pushState(null, '', '#' + elementId)
      } else {
        showCopyNotification('コピーに失敗しました')
      }
    } catch {
      showCopyNotification('コピーに失敗しました')
    } finally {
      document.body.removeChild(textArea)
    }
  }

  // 日付表示用フォーマット
  const formatDateForDisplay = (date: Date): string => {
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const hours = String(date.getHours()).padStart(2, '0')
    const minutes = String(date.getMinutes()).padStart(2, '0')
    return `${year}-${month}-${day} ${hours}:${minutes}`
  }

  // 経過時間をフォーマット
  const formatElapsedTime = (seconds: number | null): string => {
    if (seconds === null || seconds < 0) return ''
    if (seconds < 60) {
      return `${Math.floor(seconds)}秒`
    }
    const minutes = Math.floor(seconds / 60)
    const remainingSeconds = Math.floor(seconds % 60)
    return `${minutes}分${remainingSeconds}秒`
  }

  // 進捗段階のテキストを取得
  const getStageText = (stage: string | null, progress: number): string => {
    if (stage) return stage
    // stageが未設定の場合、progressから推測
    // バックエンドの推定と同期: 0-40%:取得, 40-70%:処理, 70-90%:描画, 90-100%:生成
    if (progress <= 10) return 'データベース接続中...'
    if (progress <= 40) return 'データ取得中...'
    if (progress <= 70) return 'データ処理中...'
    if (progress <= 90) return 'グラフ描画中...'
    return '画像生成中...'
  }

  // 進捗状況のテキストを取得
  const getProgressText = (
    status: string,
    progress: number,
    elapsedSeconds: number | null,
    stage: string | null
  ): { main: string; sub: string } => {
    switch (status) {
      case 'pending':
        return { main: '待機中...', sub: 'ジョブキューに追加されました' }
      case 'processing': {
        const elapsed = formatElapsedTime(elapsedSeconds)
        const stageText = getStageText(stage, progress)
        return {
          main: stageText,
          sub: elapsed ? `経過時間: ${elapsed}` : ''
        }
      }
      default:
        return { main: '', sub: '' }
    }
  }

  return (
    <>
      <div className="box" id="graph">
        <div className={styles.sectionHeader}>
          <h2 className="title is-4">
            <span className="icon" style={{ marginRight: '0.5em' }}>
              <i className="fas fa-chart-line"></i>
            </span>
            <span style={{ whiteSpace: 'nowrap' }}>グラフ</span>
            <span className="subtitle is-6 ml-2" style={{
              display: 'flex',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: '0.25rem'
            }}>
              <span>(</span>
              <span style={{ whiteSpace: 'nowrap' }}>{formatDateForDisplay(dateRange.start)}</span>
              <span style={{ whiteSpace: 'nowrap' }}>～</span>
              <span style={{ whiteSpace: 'nowrap' }}>{formatDateForDisplay(dateRange.end)}</span>
              <span>)</span>
            </span>
            <i
              className={`fas fa-link ${styles.permalinkIcon}`}
              onClick={() => copyPermalink('graph')}
              title="パーマリンクをコピー"
            />
          </h2>
        </div>

        <div className="columns is-multiline">
          {graphs.map(graph => {
            const job = jobs[graph.name]
            const is3D = graph.name.includes('3d')

            // ジョブの状態を取得
            const isJobLoading = (!job || job.status === 'pending' || job.status === 'processing') && !job?.isRetrying
            const hasError = job?.status === 'failed' || job?.status === 'timeout'
            const progress = job?.progress ?? 0

            // コンテナ高さを計算
            const containerWidth = containerWidths[graph.endpoint]
            let containerHeight: number
            if (containerWidth) {
              containerHeight = calculateActualHeight(graph, containerWidth)
            } else {
              const [width, height] = graph.size
              const aspectRatio = height / width
              const estimatedWidth = is3D ? 600 : 350
              containerHeight = estimatedWidth * aspectRatio
            }

            const cardPadding = 16
            const cardHeight = containerHeight + cardPadding

            return (
              <div
                key={graph.endpoint}
                className={is3D ? 'column is-full' : 'column is-half'}
                ref={(el) => { containerRefs.current[graph.endpoint] = el }}
              >
                <div className="card" style={{ height: `${cardHeight}px` }}>
                  <div className="card-content" style={{
                    padding: '0.5rem',
                    height: '100%',
                    display: 'flex',
                    flexDirection: 'column'
                  }}>
                    <div className="image-container" style={{
                      height: `${containerHeight}px`,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      position: 'relative',
                      overflow: 'hidden',
                      flex: '1 1 auto'
                    }}>
                      {/* ローディング表示（進捗バー付き） */}
                      {isJobLoading && (
                        <div className={`has-text-centered ${styles.loadingContainer}`} style={{ width: '80%' }}>
                          <p className="is-size-6 has-text-weight-semibold mb-2">
                            {graph.title}
                          </p>
                          <div className={styles.loaderWrapper}>
                            <div className="loader"></div>
                          </div>
                          <progress
                            className="progress is-primary is-small mt-2"
                            value={progress}
                            max="100"
                          />
                          {(() => {
                            const progressInfo = getProgressText(
                              job?.status ?? 'pending',
                              progress,
                              job?.elapsedSeconds ?? null,
                              job?.stage ?? null
                            )
                            return (
                              <>
                                <p className={`mt-1 is-size-7 has-text-weight-medium ${styles.pulsingText}`}>
                                  {progressInfo.main}
                                </p>
                                {progressInfo.sub && (
                                  <p className="is-size-7 has-text-grey">
                                    {progressInfo.sub}
                                  </p>
                                )}
                              </>
                            )
                          })()}
                        </div>
                      )}

                      {/* リトライ中表示 */}
                      {job?.isRetrying && (
                        <div className={`has-text-centered ${styles.loadingContainer}`} style={{ width: '80%' }}>
                          <p className="is-size-6 has-text-weight-semibold mb-2">
                            {graph.title}
                          </p>
                          <div className={styles.loaderWrapper}>
                            <div className="loader"></div>
                          </div>
                          <progress
                            className="progress is-warning is-small mt-2"
                            value={progress}
                            max="100"
                          />
                          <p className={`mt-1 is-size-7 has-text-weight-medium ${styles.pulsingText}`}>
                            🔄 リトライ中...
                          </p>
                          <p className="is-size-7 has-text-grey">
                            接続を再試行しています
                          </p>
                        </div>
                      )}

                      {/* エラー表示（リトライ後も失敗した場合） */}
                      {hasError && !job?.isRetrying && (
                        <figure
                          className="image"
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            height: '100%',
                            margin: 0
                          }}
                        >
                          {/* E2Eテスト用: alt属性を持つimg要素（1x1透明画像） */}
                          <img
                            src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
                            alt={graph.title}
                            style={{ position: 'absolute', width: 1, height: 1, opacity: 0 }}
                          />
                          <div className="notification is-danger is-light" style={{ textAlign: 'center', width: '80%' }}>
                            <p className="is-size-5 mb-2">❌ エラー</p>
                            <p className="is-size-7 has-text-grey mb-3">
                              {job?.error || 'グラフの生成に失敗しました'}
                            </p>
                            <button
                              className="button is-small is-danger"
                              onClick={() => reloadJob(graph.name)}
                            >
                              <span className="icon">
                                <i className="fas fa-redo"></i>
                              </span>
                              <span>リロード</span>
                            </button>
                          </div>
                        </figure>
                      )}

                      {/* 画像表示 */}
                      {job?.status === 'completed' && job.resultUrl && (
                        <figure
                          className="image"
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            height: '100%',
                            margin: 0
                          }}
                        >
                          <img
                            key={job.resultUrl}
                            src={job.resultUrl}
                            alt={graph.title}
                            style={{
                              width: '100%',
                              height: '100%',
                              objectFit: 'contain',
                              cursor: 'pointer'
                            }}
                            onClick={() => onImageClick(job.resultUrl!)}
                            onError={() => reloadJob(graph.name)}
                            loading="eager"
                          />
                        </figure>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </div>
      <div ref={notificationRef} className={styles.copyNotification}></div>
    </>
  )
}

export default GraphDisplay
