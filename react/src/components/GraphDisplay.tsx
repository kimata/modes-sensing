import { useState, useEffect, useRef } from 'react'
import styles from './GraphDisplay.module.css'

interface GraphDisplayProps {
  dateRange: {
    start: Date
    end: Date
  }
  onImageClick: (imageUrl: string) => void
}

interface GraphInfo {
  endpoint: string
  title: string
  filename: string
}

const graphs: GraphInfo[] = [
  { endpoint: '/modes-sensing/api/graph/scatter_2d', title: '2D散布図', filename: 'scatter_2d.png' },
  { endpoint: '/modes-sensing/api/graph/contour_2d', title: '2D等高線プロット', filename: 'contour.png' },
  { endpoint: '/modes-sensing/api/graph/density', title: '密度プロット', filename: 'density.png' },
  { endpoint: '/modes-sensing/api/graph/heatmap', title: 'ヒートマップ', filename: 'heatmap.png' },
  { endpoint: '/modes-sensing/api/graph/scatter_3d', title: '3D散布図', filename: 'scatter_3d.png' },
  { endpoint: '/modes-sensing/api/graph/contour_3d', title: '3D等高線プロット', filename: 'contour_3d.png' }
]

const GraphDisplay: React.FC<GraphDisplayProps> = ({ dateRange, onImageClick }) => {
  const [loading, setLoading] = useState<{ [key: string]: boolean }>({})
  const [errors, setErrors] = useState<{ [key: string]: string }>({})
  const [imageUrls, setImageUrls] = useState<{ [key: string]: string }>({})
  const [imageVersion, setImageVersion] = useState(0) // 画像の更新を強制するためのバージョン
  const [retryCount, setRetryCount] = useState<{ [key: string]: number }>({}) // リトライ回数
  const [loadingTimers, setLoadingTimers] = useState<{ [key: string]: number }>({}) // タイムアウトタイマー
  const notificationRef = useRef<HTMLDivElement>(null)

  const formatDateForAPI = (date: Date): string => {
    // UTC時間として送信
    return JSON.stringify(date.toISOString())
  }

  const formatDateForDisplay = (date: Date): string => {
    // ローカル時間で表示
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const hours = String(date.getHours()).padStart(2, '0')
    const minutes = String(date.getMinutes()).padStart(2, '0')

    return `${year}-${month}-${day} ${hours}:${minutes}`
  }

  const getImageUrl = (graph: GraphInfo, version?: number) => {
    const params = new URLSearchParams({
      start: formatDateForAPI(dateRange.start),
      end: formatDateForAPI(dateRange.end),
      v: (version !== undefined ? version : imageVersion).toString()  // キャッシュバスターを追加
    })
    return `${graph.endpoint}?${params}`
  }

  // ページ読み込み時にハッシュがあれば該当要素にスクロール
  useEffect(() => {
    if (window.location.hash === '#graph') {
      const element = document.getElementById('graph')
      if (element) {
        setTimeout(() => {
          element.scrollIntoView({ behavior: 'smooth', block: 'start' })
        }, 500)
      }
    }
  }, [])

  // パーマリンクコピー用の通知表示
  const showCopyNotification = (message: string) => {
    if (!notificationRef.current) return

    notificationRef.current.textContent = message
    notificationRef.current.classList.add(styles.show)

    setTimeout(() => {
      notificationRef.current?.classList.remove(styles.show)
    }, 3000)
  }

  // パーマリンクをコピーする関数
  const copyPermalink = (elementId: string) => {
    const currentUrl = window.location.origin + window.location.pathname
    const permalink = currentUrl + '#' + elementId

    // Clipboard APIが利用可能かチェック
    if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
      navigator.clipboard.writeText(permalink).then(() => {
        showCopyNotification('パーマリンクをコピーしました')
        window.history.pushState(null, '', '#' + elementId)
      }).catch(() => {
        // Clipboard APIが失敗した場合のフォールバック
        fallbackCopyToClipboard(permalink, elementId)
      })
    } else {
      // Clipboard APIが利用できない場合のフォールバック
      fallbackCopyToClipboard(permalink, elementId)
    }
  }

  // フォールバック用のコピー関数
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

  // 画像の読み込み状態を管理
  const handleImageLoad = (key: string) => {
    // タイマーをクリア
    if (loadingTimers[key]) {
      clearTimeout(loadingTimers[key])
      setLoadingTimers(prev => {
        const newTimers = { ...prev }
        delete newTimers[key]
        return newTimers
      })
    }
    setLoading(prev => ({ ...prev, [key]: false }))
    // リトライ回数をリセット
    setRetryCount(prev => ({ ...prev, [key]: 0 }))
  }

  const handleImageError = (key: string, title: string) => {
    // タイマーをクリア
    if (loadingTimers[key]) {
      clearTimeout(loadingTimers[key])
      setLoadingTimers(prev => {
        const newTimers = { ...prev }
        delete newTimers[key]
        return newTimers
      })
    }
    setLoading(prev => ({ ...prev, [key]: false }))
    setErrors(prev => ({ ...prev, [key]: `${title}の読み込みに失敗しました` }))
  }

  // 画像の再読み込みを行う
  const retryImageLoad = (key: string) => {
    const currentRetryCount = retryCount[key] || 0

    // 最大2回までリトライ（計3回試行：初回 + リトライ2回）
    if (currentRetryCount < 2) {
      setRetryCount(prev => ({ ...prev, [key]: currentRetryCount + 1 }))

      // 新しいバージョンでURLを更新
      const newVersion = imageVersion + 1
      setImageVersion(newVersion)

      // 該当する画像のURLを新しいバージョンで更新
      const graph = graphs.find(g => g.endpoint === key)
      if (graph) {
        setImageUrls(prev => ({ ...prev, [key]: getImageUrl(graph, newVersion) }))
      }
    } else {
      setLoading(prev => ({ ...prev, [key]: false }))
      setErrors(prev => ({ ...prev, [key]: '画像の読み込みに失敗しました（30秒でタイムアウト）' }))
    }
  }

  useEffect(() => {
    // 既存のタイマーをクリア
    Object.values(loadingTimers).forEach(timer => clearTimeout(timer))

    // 全てのグラフに対してURLを設定し、読み込み状態を初期化
    const newImageUrls: { [key: string]: string } = {}
    const newLoadingState: { [key: string]: boolean } = {}
    const newErrorState: { [key: string]: string } = {}

    // バージョンを更新して画像の再読み込みを促す
    const newVersion = imageVersion + 1
    setImageVersion(newVersion)

    graphs.forEach(graph => {
      const key = graph.endpoint
      newImageUrls[key] = getImageUrl(graph, newVersion)
      newLoadingState[key] = true  // 初期状態は読み込み中
      newErrorState[key] = ''
    })

    // 状態を一括更新
    setImageUrls(newImageUrls)
    setLoading(newLoadingState)
    setErrors(newErrorState)
    setRetryCount({}) // リトライ回数をリセット
    setLoadingTimers({}) // タイマーをリセット

    // クリーンアップ関数
    return () => {
      // eslint-disable-next-line react-hooks/exhaustive-deps
      Object.values(loadingTimers).forEach(timer => clearTimeout(timer))
    }
  }, [dateRange])

  // 画像読み込みタイムアウト監視用のuseEffect
  useEffect(() => {
    const newTimers: { [key: string]: number } = {}

    graphs.forEach(graph => {
      const key = graph.endpoint
      if (loading[key] && !loadingTimers[key]) {
        // 10秒のタイムアウトを設定（既存のタイマーがない場合のみ）
        newTimers[key] = window.setTimeout(() => {
          retryImageLoad(key)
        }, 10000)
      }
    })

    if (Object.keys(newTimers).length > 0) {
      setLoadingTimers(prev => ({ ...prev, ...newTimers }))
    }

    // クリーンアップ関数
    return () => {
      Object.values(newTimers).forEach(timer => clearTimeout(timer))
    }
  }, [loading])

  return (
    <>
      <div className="box" id="graph">
        <div className={styles.sectionHeader}>
          <h2 className="title is-4">
            <span className="icon" style={{ marginRight: '0.5em' }}>
              <i className="fas fa-chart-line"></i>
            </span>
            グラフ
            <span className="subtitle is-6 ml-2">
              ({formatDateForDisplay(dateRange.start)} ～ {formatDateForDisplay(dateRange.end)})
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

          return (
            <div key={key} className={graph.endpoint.includes('3d') ? 'column is-full' : 'column is-half'}>
              <div className="card">
                <div className="card-content">
                  <div className="image-container" style={{ minHeight: '200px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    {isLoading && (
                      <div className="has-text-centered">
                        <div className="loader"></div>
                        <p className="mt-2">読み込み中...</p>
                      </div>
                    )}

                    {error && (
                      <div className="notification is-danger is-light">
                        <div>{error}</div>
                        <button
                          className="button is-small is-danger mt-2"
                          onClick={() => {
                            setErrors(prev => ({ ...prev, [key]: '' }))
                            setRetryCount(prev => ({ ...prev, [key]: 0 }))
                            setLoading(prev => ({ ...prev, [key]: true }))

                            // 新しいバージョンでURLを更新
                            const newVersion = imageVersion + 1
                            setImageVersion(newVersion)

                            // 該当する画像のURLを新しいバージョンで更新
                            const graph = graphs.find(g => g.endpoint === key)
                            if (graph) {
                              setImageUrls(prev => ({ ...prev, [key]: getImageUrl(graph, newVersion) }))
                            }
                          }}
                        >
                          <span className="icon">
                            <i className="fas fa-redo"></i>
                          </span>
                          <span>リロード</span>
                        </button>
                      </div>
                    )}

                    {imageUrl && (
                      <figure className="image" style={{ display: isLoading ? 'none' : 'block' }}>
                        <img
                          src={imageUrl}
                          alt={graph.title}
                          style={{
                            width: '100%',     // card-contentの幅に合わせる
                            height: 'auto',    // アスペクト比を保持
                            cursor: 'pointer'
                          }}
                          onClick={() => onImageClick(imageUrl)}
                          onLoad={() => handleImageLoad(key)}
                          onError={() => handleImageError(key, graph.title)}
                          key={imageVersion} // バージョンが変わると画像を再読み込み
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
