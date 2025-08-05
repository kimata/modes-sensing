import { useState, useEffect, useRef } from 'react'
import styles from './GraphDisplay.module.css'

// タイムアウトとリトライの設定
const IMAGE_LOAD_TIMEOUT = 10000 // 10秒
const MAX_RETRY_COUNT = 2 // 最大2回リトライ

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
  { endpoint: '/modes-sensing/api/graph/temperature_timeseries', title: '高度別温度時系列', filename: 'temperature_timeseries.png' },
  { endpoint: '/modes-sensing/api/graph/scatter_3d', title: '3D散布図', filename: 'scatter_3d.png' },
  { endpoint: '/modes-sensing/api/graph/contour_3d', title: '3D等高線プロット', filename: 'contour_3d.png' }
]

const GraphDisplay: React.FC<GraphDisplayProps> = ({ dateRange, onImageClick }) => {
  // 初期状態を設定
  const initializeState = () => {
    const initialLoading: { [key: string]: boolean } = {}
    const initialErrors: { [key: string]: string } = {}
    const initialImageUrls: { [key: string]: string } = {}

    graphs.forEach(graph => {
      const key = graph.endpoint
      initialLoading[key] = true
      initialErrors[key] = ''
      initialImageUrls[key] = ''
    })

    return { initialLoading, initialErrors, initialImageUrls }
  }

  const { initialLoading, initialErrors, initialImageUrls } = initializeState()

  const [loading, setLoading] = useState<{ [key: string]: boolean }>(initialLoading)
  const [errors, setErrors] = useState<{ [key: string]: string }>(initialErrors)
  const [imageUrls, setImageUrls] = useState<{ [key: string]: string }>(initialImageUrls)
  const [imageVersion, setImageVersion] = useState(0)
  const [retryCount, setRetryCount] = useState<{ [key: string]: number }>({})
  const [loadingTimers, setLoadingTimers] = useState<{ [key: string]: number }>({})
  const imageRefs = useRef<{ [key: string]: HTMLImageElement | null }>({})

  // 画像要素の実際の読み込み状態をチェックする関数
  const checkImageLoadingState = (key: string): boolean => {
    const img = imageRefs.current[key]
    return img ? (img.complete && img.naturalWidth > 0) : false
  }

  // 全画像の状態を定期的にチェックして、onLoadイベント消失を補完
  const checkAllImagesStatus = () => {
    graphs.forEach(graph => {
      const key = graph.endpoint
      // loading状態かつ実際には読み込み完了している場合のみ処理
      if (loading[key] && checkImageLoadingState(key)) {
        console.log(`[checkAllImagesStatus] ${key}: detected loaded image with loading=true, fixing state`)
        handleImageLoad(key)
      }
    })
  }
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

  // 初回マウント時に画像URLを設定
  useEffect(() => {
    const newImageUrls: { [key: string]: string } = {}
    graphs.forEach(graph => {
      const key = graph.endpoint
      newImageUrls[key] = getImageUrl(graph, 0)
    })
    setImageUrls(newImageUrls)
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

  // 画像の読み込み状態を管理（Refベース）
  const handleImageLoad = (key: string) => {
    const img = imageRefs.current[key]
    console.log(`[handleImageLoad] ${key}: img exists=${!!img}, complete=${img?.complete}, naturalWidth=${img?.naturalWidth}`)

    if (img && img.complete && img.naturalWidth > 0) {
      // タイマーをクリア
      if (loadingTimers[key]) {
        console.log(`[handleImageLoad] ${key}: clearing timeout timer`)
        clearTimeout(loadingTimers[key])
        setLoadingTimers(prev => {
          const newTimers = { ...prev }
          delete newTimers[key]
          return newTimers
        })
      }
      console.log(`[handleImageLoad] ${key}: setting loading to false`)
      // 更新時に既にfalseかチェック
      setLoading(prev => {
        if (prev[key] === false) {
          console.log(`[handleImageLoad] ${key}: already false, skipping state update`)
          return prev
        }
        console.log(`[handleImageLoad] ${key}: updating state from true to false`)
        return { ...prev, [key]: false }
      })
      setRetryCount(prev => ({ ...prev, [key]: 0 }))
    } else {
      console.log(`[handleImageLoad] ${key}: invalid image state, not marking as loaded`)
    }
  }

  const handleImageError = (key: string, title: string) => {
    const img = imageRefs.current[key]
    console.log(`[handleImageError] ${key}: image error occurred`)
    console.log(`[handleImageError] ${key}: img.src = ${img?.src}`)
    console.log(`[handleImageError] ${key}: img.complete = ${img?.complete}`)
    console.log(`[handleImageError] ${key}: img.naturalWidth = ${img?.naturalWidth}`)

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
  const retryImageLoad = (key: string, currentRetryCount?: number) => {
    const actualRetryCount = currentRetryCount !== undefined ? currentRetryCount : (retryCount[key] || 0)
    console.log(`[retryImageLoad] ${key}: retry count = ${actualRetryCount}`)

    // 最大2回までリトライ（計3回試行：初回 + リトライ2回）
    if (actualRetryCount < MAX_RETRY_COUNT) {

      // リトライ回数を更新
      const nextRetryCount = actualRetryCount + 1
      setRetryCount(prev => ({ ...prev, [key]: nextRetryCount }))

      // 新しいバージョンでURLを更新
      const newVersion = imageVersion + 1
      setImageVersion(newVersion)

      // 該当する画像のURLを新しいバージョンで更新
      const graph = graphs.find(g => g.endpoint === key)
      if (graph) {
        const newUrl = getImageUrl(graph, newVersion)
        console.log(`[retryImageLoad] ${key}: setting new URL = ${newUrl}`)
        setImageUrls(prev => ({ ...prev, [key]: newUrl }))
        setLoading(prev => ({ ...prev, [key]: true }))
        setErrors(prev => ({ ...prev, [key]: '' }))

        // 新しいタイムアウトタイマーを設定
        const newTimer = window.setTimeout(() => {
          const img = imageRefs.current[key]
          console.log(`[Timeout check] ${key}: img exists=${!!img}, complete=${img?.complete}, naturalWidth=${img?.naturalWidth}`)
          if (!img || !img.complete || img.naturalWidth === 0) {
            retryImageLoad(key, nextRetryCount)
          } else {
            handleImageLoad(key)
          }
        }, IMAGE_LOAD_TIMEOUT)

        setLoadingTimers(prev => ({ ...prev, [key]: newTimer }))
      }
    } else {
      const img = imageRefs.current[key]
      console.log(`[retryImageLoad] ${key}: max retry count reached (${actualRetryCount}), giving up`)
      console.log(`[retryImageLoad] ${key}: final state - img.src = ${img?.src}`)
      console.log(`[retryImageLoad] ${key}: final state - img.complete = ${img?.complete}`)
      console.log(`[retryImageLoad] ${key}: final state - img.naturalWidth = ${img?.naturalWidth}`)
      setLoading(prev => ({ ...prev, [key]: false }))
      setErrors(prev => ({ ...prev, [key]: `画像の読み込みに失敗しました（${(MAX_RETRY_COUNT + 1) * IMAGE_LOAD_TIMEOUT / 1000}秒でタイムアウト）` }))
    }
  }

  useEffect(() => {

    // 既存のタイマーをクリア
    setLoadingTimers(prev => {
      Object.values(prev).forEach(timer => clearTimeout(timer))
      return {}
    })

    // バージョンを更新して画像の再読み込みを促す
    const newVersion = imageVersion + 1
    setImageVersion(newVersion)

    // 状態更新を同期的に処理するため、一度の更新にまとめる
    const newImageUrls: { [key: string]: string } = {}
    const newLoadingState: { [key: string]: boolean } = {}
    const newErrorState: { [key: string]: string } = {}
    const newTimers: { [key: string]: number } = {}

    graphs.forEach((graph, index) => {
      const key = graph.endpoint
      newImageUrls[key] = getImageUrl(graph, newVersion)
      newLoadingState[key] = true
      newErrorState[key] = ''

      // 画像読み込みのタイミングを少しずらす（同時リクエスト制限対策）
      const delayMs = index * 10 // 10ms間隔でずらす

      // 各画像に対してタイムアウトタイマーを設定（画像要素の実際の状態をチェック）
      newTimers[key] = window.setTimeout(() => {
        const img = imageRefs.current[key]
        console.log(`[Initial timeout check] ${key}: img exists=${!!img}, complete=${img?.complete}, naturalWidth=${img?.naturalWidth}`)
        if (!img || !img.complete || img.naturalWidth === 0) {
          retryImageLoad(key, 0)  // 初回は0からスタート
        } else {
          // 実際には読み込み完了していた場合
          handleImageLoad(key)
        }
      }, IMAGE_LOAD_TIMEOUT + delayMs)
    })

    // 状態を一括更新
    setImageUrls(newImageUrls)
    setLoading(newLoadingState)
    setErrors(newErrorState)
    setRetryCount({})
    setLoadingTimers(newTimers)

    // 定期的な画像状態チェックを開始（onLoadイベント消失を補完）
    const statusCheckInterval = setInterval(() => {
      checkAllImagesStatus()
    }, 1000) // 1秒ごとにチェック

    // クリーンアップ関数
    return () => {
      Object.values(newTimers).forEach(timer => clearTimeout(timer))
      clearInterval(statusCheckInterval)
    }
  }, [dateRange])

  // 画像読み込みタイムアウト監視は dateRange useEffect内で統合済み

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
                          ref={(el) => { imageRefs.current[key] = el }}
                          src={imageUrl}
                          alt={graph.title}
                          style={{
                            width: '100%',
                            height: 'auto',
                            cursor: 'pointer'
                          }}
                          onClick={() => onImageClick(imageUrl)}
                          onLoad={() => {
                            console.log(`[img onLoad] ${key}: onLoad event fired`)
                            console.log(`[img onLoad] ${key}: current loading state = ${loading[key]}`)
                            handleImageLoad(key)
                          }}
                          onError={() => {
                            console.log(`[img onError] ${key}: onError event fired`)
                            console.log(`[img onError] ${key}: current loading state = ${loading[key]}`)
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
