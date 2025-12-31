import { useState, useEffect, useRef, useLayoutEffect } from 'react'
import styles from './GraphDisplay.module.css'

interface GraphDisplayProps {
  dateRange: {
    start: Date
    end: Date
  }
  limitAltitude: boolean
  onImageClick: (imageUrl: string) => void
}

interface GraphInfo {
  endpoint: string
  title: string
  filename: string
  size: [number, number]  // [width, height] in pixels
}

// graph.pyのGRAPH_DEF_MAPのsize定義をコピー
const graphs: GraphInfo[] = [
  { endpoint: '/modes-sensing/api/graph/scatter_2d', title: '2D散布図', filename: 'scatter_2d.png', size: [2400, 1600] },
  { endpoint: '/modes-sensing/api/graph/contour_2d', title: '2D等高線プロット', filename: 'contour.png', size: [2400, 1600] },
  { endpoint: '/modes-sensing/api/graph/density', title: '密度プロット', filename: 'density.png', size: [2400, 1600] },
  { endpoint: '/modes-sensing/api/graph/heatmap', title: 'ヒートマップ', filename: 'heatmap.png', size: [2400, 1600] },
  { endpoint: '/modes-sensing/api/graph/temperature', title: '高度別温度時系列', filename: 'temperature.png', size: [2400, 1600] },
  { endpoint: '/modes-sensing/api/graph/wind_direction', title: '風向・風速分布', filename: 'wind_direction.png', size: [2400, 1600] },
  { endpoint: '/modes-sensing/api/graph/scatter_3d', title: '3D散布図', filename: 'scatter_3d.png', size: [2800, 2800] },
  { endpoint: '/modes-sensing/api/graph/contour_3d', title: '3D等高線プロット', filename: 'contour_3d.png', size: [2800, 2800] }
]

// 設定キーを生成する関数（10分単位でキャッシュ可能）
const generateSettingsKey = (start: Date, end: Date, limitAltitude: boolean): string => {
  // 10分単位のタイムスロットを計算（将来のキャッシュ用）
  const tenMinutesInMs = 10 * 60 * 1000
  const timeSlot = Math.floor(Date.now() / tenMinutesInMs) * tenMinutesInMs
  // 期間と高度制限を含むユニークキー
  return `${start.getTime()}-${end.getTime()}-${limitAltitude}-${timeSlot}`
}

// 画像URLを生成する関数
const buildImageUrl = (
  graph: GraphInfo,
  start: Date,
  end: Date,
  limitAltitude: boolean,
  forceReload: boolean = false
): string => {
  const params = new URLSearchParams()
  params.set('start', JSON.stringify(start.toISOString()))
  params.set('end', JSON.stringify(end.toISOString()))
  params.set('limit_altitude', limitAltitude ? 'true' : 'false')

  // キャッシュバスター: 設定が同じなら同じURL（10分間隔でキャッシュ可能）
  const cacheKey = generateSettingsKey(start, end, limitAltitude)
  params.set('_t', cacheKey)

  // 強制リロード時はランダム値を追加
  if (forceReload) {
    params.set('_r', Math.random().toString(36).substring(2, 11))
  }

  const url = `${graph.endpoint}?${params.toString()}`

  // デバッグログ
  const periodDays = (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24)
  console.log(`[buildImageUrl] ${graph.endpoint}:`, {
    periodDays: periodDays.toFixed(2),
    start: start.toISOString(),
    end: end.toISOString(),
    limitAltitude,
    url
  })

  return url
}

// コンテナ高さを計算するヘルパー関数
const calculateActualHeight = (graph: GraphInfo, containerWidth: number): number => {
  const [imageWidth, imageHeight] = graph.size
  const aspectRatio = imageHeight / imageWidth
  return containerWidth * aspectRatio
}

const GraphDisplay: React.FC<GraphDisplayProps> = ({ dateRange, limitAltitude, onImageClick }) => {
  // 画像の状態管理
  const [imageUrls, setImageUrls] = useState<Record<string, string>>({})
  const [loading, setLoading] = useState<Record<string, boolean>>({})
  const [errors, setErrors] = useState<Record<string, string>>({})

  // コンテナ幅の追跡
  const containerRefs = useRef<Record<string, HTMLDivElement | null>>({})
  const [containerWidths, setContainerWidths] = useState<Record<string, number>>({})

  // 通知用ref
  const notificationRef = useRef<HTMLDivElement>(null)

  // 前回の設定を追跡するためのref（キー文字列として保存）
  const lastSettingsRef = useRef<string>('')

  // 現在の設定からキーを生成
  const currentSettingsKey = `${dateRange.start.getTime()}-${dateRange.end.getTime()}-${limitAltitude}`

  // デバッグ: 設定変更を検出
  useEffect(() => {
    const periodDays = (dateRange.end.getTime() - dateRange.start.getTime()) / (1000 * 60 * 60 * 24)
    console.log(`[GraphDisplay] Settings:`, {
      periodDays: periodDays.toFixed(2),
      start: dateRange.start.toISOString(),
      end: dateRange.end.toISOString(),
      limitAltitude,
      currentKey: currentSettingsKey,
      lastKey: lastSettingsRef.current,
      isNewSettings: currentSettingsKey !== lastSettingsRef.current
    })
  }, [dateRange, limitAltitude, currentSettingsKey])

  // 設定が変わったら画像URLを更新
  useEffect(() => {
    // 設定が変わっていない場合はスキップ
    if (lastSettingsRef.current === currentSettingsKey) {
      console.log(`[GraphDisplay useEffect] Skipping - same settings`)
      return
    }

    console.log(`[GraphDisplay useEffect] Settings changed, updating URLs`)
    console.log(`  Previous: ${lastSettingsRef.current}`)
    console.log(`  Current: ${currentSettingsKey}`)

    // 設定を更新
    lastSettingsRef.current = currentSettingsKey

    // 全グラフのURLを生成
    const newUrls: Record<string, string> = {}
    const newLoading: Record<string, boolean> = {}

    graphs.forEach(graph => {
      const url = buildImageUrl(graph, dateRange.start, dateRange.end, limitAltitude)
      newUrls[graph.endpoint] = url
      newLoading[graph.endpoint] = true
    })

    // 状態を更新
    setImageUrls(newUrls)
    setLoading(newLoading)
    setErrors({})

  }, [dateRange.start, dateRange.end, limitAltitude, currentSettingsKey])

  // 画像読み込み完了ハンドラ
  const handleImageLoad = (endpoint: string) => {
    console.log(`[GraphDisplay] Image loaded: ${endpoint}`)
    setLoading(prev => ({ ...prev, [endpoint]: false }))
    setErrors(prev => ({ ...prev, [endpoint]: '' }))
  }

  // 画像読み込みエラーハンドラ
  const handleImageError = (endpoint: string, title: string) => {
    console.error(`[GraphDisplay] Image error: ${endpoint}`)
    setLoading(prev => ({ ...prev, [endpoint]: false }))
    setErrors(prev => ({ ...prev, [endpoint]: `${title}の読み込みに失敗しました` }))
  }

  // 画像リロードハンドラ
  const handleReload = (endpoint: string) => {
    const graph = graphs.find(g => g.endpoint === endpoint)
    if (!graph) return

    console.log(`[GraphDisplay] Reloading: ${endpoint}`)
    setLoading(prev => ({ ...prev, [endpoint]: true }))
    setErrors(prev => ({ ...prev, [endpoint]: '' }))

    // 強制リロードでURLを更新
    const newUrl = buildImageUrl(graph, dateRange.start, dateRange.end, limitAltitude, true)
    setImageUrls(prev => ({ ...prev, [endpoint]: newUrl }))
  }

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
            const endpoint = graph.endpoint
            const isLoading = loading[endpoint] ?? true
            const error = errors[endpoint]
            const imageUrl = imageUrls[endpoint]
            const is3D = endpoint.includes('3d')

            // コンテナ高さを計算
            const containerWidth = containerWidths[endpoint]
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
                key={endpoint}
                className={is3D ? 'column is-full' : 'column is-half'}
                ref={(el) => { containerRefs.current[endpoint] = el }}
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
                      {/* ローディング表示 */}
                      {isLoading && (
                        <div className="has-text-centered">
                          <div className="loader"></div>
                          <p className="mt-2">グラフの生成中...</p>
                        </div>
                      )}

                      {/* エラー表示 */}
                      {error && !isLoading && (
                        <div className="notification is-danger is-light">
                          <div>{error}</div>
                          <button
                            className="button is-small is-danger mt-2"
                            onClick={() => handleReload(endpoint)}
                          >
                            <span className="icon">
                              <i className="fas fa-redo"></i>
                            </span>
                            <span>リロード</span>
                          </button>
                        </div>
                      )}

                      {/* 画像表示 */}
                      {imageUrl && (
                        <figure
                          className="image"
                          style={{
                            display: isLoading ? 'none' : 'flex',
                            alignItems: 'center',
                            justifyContent: 'center',
                            height: '100%',
                            margin: 0
                          }}
                        >
                          <img
                            key={imageUrl}  // URLが変わったら強制的にリマウント
                            src={imageUrl}
                            alt={graph.title}
                            style={{
                              width: '100%',
                              height: '100%',
                              objectFit: 'contain',
                              cursor: 'pointer'
                            }}
                            onClick={() => onImageClick(imageUrl)}
                            onLoad={() => handleImageLoad(endpoint)}
                            onError={() => handleImageError(endpoint, graph.title)}
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
