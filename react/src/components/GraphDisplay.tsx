import { useState, useEffect, useRef, useLayoutEffect, useCallback } from 'react'
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

// Helper function moved outside component
const calculateActualHeight = (graph: GraphInfo, containerWidth: number): number => {
  const [imageWidth, imageHeight] = graph.size
  const aspectRatio = imageHeight / imageWidth
  return containerWidth * aspectRatio
}

const GraphDisplay: React.FC<GraphDisplayProps> = ({ dateRange, limitAltitude, onImageClick }) => {
  // シンプルな状態管理のみ
  const [loading, setLoading] = useState<{ [key: string]: boolean }>(() => {
    const initial: { [key: string]: boolean } = {}
    graphs.forEach(graph => {
      initial[graph.endpoint] = true
    })
    return initial
  })

  const [errors, setErrors] = useState<{ [key: string]: string }>({})
  const [imageUrls, setImageUrls] = useState<{ [key: string]: string }>({})
  const containerRefs = useRef<{ [key: string]: HTMLDivElement | null }>({})
  const [containerWidths, setContainerWidths] = useState<{ [key: string]: number }>({})
  const notificationRef = useRef<HTMLDivElement>(null)
  const [isInitialLoad, setIsInitialLoad] = useState(true)
  const initialLoadCompleteRef = useRef(false)

  // シンプルなURL生成
  const getImageUrl = useCallback((graph: GraphInfo, forceReload = false) => {
    const now = new Date()
    let timestamp: number

    if (forceReload) {
      timestamp = now.getTime()
    } else {
      // 10分間隔のタイムスタンプ
      const tenMinutesInMs = 10 * 60 * 1000
      timestamp = Math.floor(now.getTime() / tenMinutesInMs) * tenMinutesInMs
    }

    const params = new URLSearchParams({
      start: JSON.stringify(dateRange.start.toISOString()),
      end: JSON.stringify(dateRange.end.toISOString()),
      limit_altitude: limitAltitude ? 'true' : 'false',
      _t: timestamp.toString(),
      ...(forceReload && { _r: Math.random().toString(36).substr(2, 9) })
    })

    // デバッグ: リクエストされる日付範囲を確認
    const periodDays = (dateRange.end.getTime() - dateRange.start.getTime()) / (1000 * 60 * 60 * 24)
    console.log(`[GraphDisplay] ${graph.endpoint}: period=${periodDays.toFixed(2)} days, start=${dateRange.start.toISOString()}, end=${dateRange.end.toISOString()}`)

    return `${graph.endpoint}?${params}`
  }, [dateRange, limitAltitude])

  // シンプルな画像ハンドラー
  const handleImageLoad = useCallback((key: string) => {
    setLoading(prev => {
      const newLoading = { ...prev, [key]: false }

      // 初回ロードが完了したらフラグを更新
      if (isInitialLoad && !initialLoadCompleteRef.current) {
        // 全てのグラフの初回ロードが完了したか確認
        const allLoaded = graphs.every(g => {
          const loadingState = newLoading[g.endpoint]
          return loadingState === false
        })

        if (allLoaded) {
          initialLoadCompleteRef.current = true
          setIsInitialLoad(false)
        }
      }

      return newLoading
    })
    setErrors(prev => ({ ...prev, [key]: '' }))
  }, [isInitialLoad])

  const handleImageError = useCallback((key: string, title: string) => {
    setLoading(prev => {
      const newLoading = { ...prev, [key]: false }

      // 初回ロードが完了したらフラグを更新（エラーでも完了とみなす）
      if (isInitialLoad && !initialLoadCompleteRef.current) {
        // 全てのグラフの初回ロードが完了したか確認
        const allLoaded = graphs.every(g => {
          const loadingState = newLoading[g.endpoint]
          return loadingState === false
        })

        if (allLoaded) {
          initialLoadCompleteRef.current = true
          setIsInitialLoad(false)
        }
      }

      return newLoading
    })
    setErrors(prev => ({ ...prev, [key]: `${title}の読み込みに失敗しました` }))
  }, [isInitialLoad])

  const handleReload = useCallback((key: string) => {
    const graph = graphs.find(g => g.endpoint === key)
    if (graph) {
      setErrors(prev => ({ ...prev, [key]: '' }))
      setLoading(prev => ({ ...prev, [key]: true }))
      // リロード時は強制リロード
      setImageUrls(prev => ({ ...prev, [key]: getImageUrl(graph, true) }))
    }
  }, [getImageUrl])

  // コンテナ幅測定
  const measureContainerWidths = () => {
    const newWidths: { [key: string]: number } = {}
    graphs.forEach(graph => {
      const key = graph.endpoint
      const container = containerRefs.current[key]
      if (container) {
        const rect = container.getBoundingClientRect()
        newWidths[key] = rect.width
      }
    })
    setContainerWidths(newWidths)
  }

  useLayoutEffect(() => {
    setTimeout(() => {
      measureContainerWidths()
    }, 100)
  }, [])

  useEffect(() => {
    const handleResize = () => {
      measureContainerWidths()
    }
    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  // 前回の日付範囲と高度設定を追跡
  const prevDateRangeRef = useRef<{ start: Date; end: Date } | null>(null)
  const prevLimitAltitudeRef = useRef<boolean | null>(null)

  // dateRangeが変更されたら画像URLを更新
  useEffect(() => {
    // 前回の日付範囲と高度設定と同じかチェック
    const prevRange = prevDateRangeRef.current
    const prevLimitAltitude = prevLimitAltitudeRef.current
    const isSameRange = prevRange &&
      prevRange.start.getTime() === dateRange.start.getTime() &&
      prevRange.end.getTime() === dateRange.end.getTime()
    const isSameLimitAltitude = prevLimitAltitude === limitAltitude

    if (isSameRange && isSameLimitAltitude && !isInitialLoad) {
      // 同じ日付範囲と高度設定の場合、ローディング状態をスキップしてすでに表示されている画像を維持
      return
    }

    // 現在の日付範囲と高度設定を記録
    prevDateRangeRef.current = { start: new Date(dateRange.start), end: new Date(dateRange.end) }
    prevLimitAltitudeRef.current = limitAltitude

    const newUrls: { [key: string]: string } = {}
    const newLoading: { [key: string]: boolean } = {}

    graphs.forEach(graph => {
      const key = graph.endpoint
      const url = getImageUrl(graph, false)
      newUrls[key] = url
      newLoading[key] = true
    })

    // 状態を同期して更新
    setImageUrls(newUrls)
    setLoading(newLoading)
    setErrors({})
  }, [dateRange, limitAltitude, getImageUrl, isInitialLoad])

  // パーマリンクコピー関数
  const showCopyNotification = (message: string) => {
    if (!notificationRef.current) return
    notificationRef.current.textContent = message
    notificationRef.current.classList.add(styles.show)
    setTimeout(() => {
      notificationRef.current?.classList.remove(styles.show)
    }, 3000)
  }

  const copyPermalink = (elementId: string) => {
    const currentUrl = window.location.origin + window.location.pathname
    const permalink = currentUrl + '#' + elementId

    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
      navigator.clipboard.writeText(permalink).then(() => {
        showCopyNotification('パーマリンクをコピーしました')
        window.history.pushState(null, '', '#' + elementId)
      }).catch(() => {
        fallbackCopyToClipboard(permalink, elementId)
      })
    } else {
      fallbackCopyToClipboard(permalink, elementId)
    }
  }

  const fallbackCopyToClipboard = (text: string, elementId: string) => {
    try {
      const textArea = document.createElement('textarea')
      textArea.value = text
      textArea.style.position = 'fixed'
      textArea.style.left = '-9999px'
      document.body.appendChild(textArea)
      textArea.focus()
      textArea.select()
      const successful = document.execCommand('copy')
      document.body.removeChild(textArea)

      if (successful) {
        showCopyNotification('パーマリンクをコピーしました')
        window.history.pushState(null, '', '#' + elementId)
      } else {
        showCopyNotification('コピーに失敗しました')
      }
    } catch (err) {
      showCopyNotification('コピーに失敗しました')
    }
  }

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
            const key = graph.endpoint
            const isLoading = loading[key]
            const error = errors[key]
            const imageUrl = imageUrls[key]

            const is3D = graph.endpoint.includes('3d')
            const actualContainerWidth = containerWidths[key]

            let calculatedHeight: number
            if (actualContainerWidth) {
              calculatedHeight = calculateActualHeight(graph, actualContainerWidth)
            } else {
              const [width, height] = graph.size
              const aspectRatio = height / width
              const estimatedWidth = is3D ? 600 : 350
              calculatedHeight = estimatedWidth * aspectRatio
            }

            const containerHeight = calculatedHeight + 'px'
            const cardPadding = 16
            const cardHeight = calculatedHeight + cardPadding + 'px'

            return (
              <div key={key} className={is3D ? 'column is-full' : 'column is-half'} ref={(el) => { containerRefs.current[key] = el }}>
                <div className="card" style={{ height: cardHeight }}>
                  <div className="card-content" style={{
                    padding: '0.5rem',
                    height: '100%',
                    display: 'flex',
                    flexDirection: 'column'
                  }}>
                    <div className="image-container" style={{
                      height: containerHeight,
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      position: 'relative',
                      overflow: 'hidden',
                      flex: '1 1 auto'
                    }}>
                      {isLoading && (
                        <div className="has-text-centered">
                          <div className="loader"></div>
                          <p className="mt-2">グラフの生成中...</p>
                        </div>
                      )}

                      {error && (
                        <div className="notification is-danger is-light">
                          <div>{error}</div>
                          <button
                            className="button is-small is-danger mt-2"
                            onClick={() => handleReload(key)}
                          >
                            <span className="icon">
                              <i className="fas fa-redo"></i>
                            </span>
                            <span>リロード</span>
                          </button>
                        </div>
                      )}

                      {imageUrl && (
                        <figure className="image" style={{
                          display: isLoading ? 'none' : 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          height: '100%',
                          margin: 0
                        }}>
                          <img
                            key={`${key}-${imageUrl}`}
                            src={imageUrl}
                            alt={graph.title}
                            style={{
                              width: '100%',
                              height: '100%',
                              objectFit: 'contain',
                              cursor: 'pointer'
                            }}
                            onClick={() => onImageClick(imageUrl)}
                            onLoad={() => handleImageLoad(key)}
                            onError={() => {
                              handleImageError(key, graph.title)
                            }}
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
