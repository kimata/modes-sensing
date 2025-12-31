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

  // 進捗状況のテキストを取得
  const getProgressText = (status: string, progress: number): string => {
    switch (status) {
      case 'pending':
        return '待機中...'
      case 'processing':
        return `生成中... ${progress}%`
      default:
        return ''
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
            const isJobLoading = !job || job.status === 'pending' || job.status === 'processing'
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
                        <div className="has-text-centered" style={{ width: '80%' }}>
                          <div className="loader"></div>
                          <progress
                            className="progress is-primary is-small mt-2"
                            value={progress}
                            max="100"
                          />
                          <p className="mt-1 is-size-7">
                            {getProgressText(job?.status ?? 'pending', progress)}
                          </p>
                        </div>
                      )}

                      {/* エラー表示 */}
                      {hasError && (
                        <div className="notification is-danger is-light">
                          <div>{job?.error || 'グラフの生成に失敗しました'}</div>
                          <button
                            className="button is-small is-danger mt-2"
                            onClick={() => reloadJob(graph.name)}
                          >
                            <span className="icon">
                              <i className="fas fa-redo"></i>
                            </span>
                            <span>リロード</span>
                          </button>
                        </div>
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
