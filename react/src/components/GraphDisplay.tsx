import { useState, useEffect, useRef, useLayoutEffect, useCallback } from 'react'
import styles from './GraphDisplay.module.css'

// タイムアウトとリトライの設定（CI環境対応）
const IMAGE_LOAD_TIMEOUT = 20000 // 20秒（CI環境向け延長）
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
  const [retryCount, setRetryCount] = useState<{ [key: string]: number }>({})
  const [loadingTimers, setLoadingTimers] = useState<{ [key: string]: number }>({})
  const imageRefs = useRef<{ [key: string]: HTMLImageElement | null }>({})
  const containerRefs = useRef<{ [key: string]: HTMLDivElement | null }>({})
  const [containerWidths, setContainerWidths] = useState<{ [key: string]: number }>({})

  // 画像要素の実際の読み込み状態をチェックする関数
  const checkImageLoadingState = (key: string): boolean => {
    const img = imageRefs.current[key]
    if (!img || !img.src) return false

    // 画像が読み込まれた場合
    if (img.complete && img.naturalWidth > 0) {
      return true
    }

    // エラー状態の場合（complete=trueだがnaturalWidth=0）
    if (img.complete && img.naturalWidth === 0) {
      return false
    }

    return false
  }

  // 状態更新の排他制御用フラグ
  const isUpdatingStateRef = useRef(false)
  const statusCheckIntervalRef = useRef<number | null>(null)

  // コンテナの実際の幅を測定する関数
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


  // 全画像の状態を定期的にチェックして、onLoadイベント消失を補完
  const checkAllImagesStatus = () => {
    // 状態更新中は重複実行を防ぐ
    if (isUpdatingStateRef.current) {
      console.log('[DEBUG] checkAllImagesStatus skipped (updating state)')
      return
    }

    // 最新の状態を取得するため、setLoadingの中で判定
    setLoading(currentLoading => {
      const updatesNeeded: string[] = []
      const currentStates: Array<{key: string, loading: boolean, imageState: boolean}> = []

      graphs.forEach(graph => {
        const key = graph.endpoint
        const imageState = checkImageLoadingState(key)
        currentStates.push({key, loading: currentLoading[key], imageState})

        // loading状態かつ実際には読み込み完了している場合のみ処理
        if (currentLoading[key] && imageState) {
          updatesNeeded.push(key)
        }
      })

      if (currentStates.length > 0) {
        console.log('[DEBUG] checkAllImagesStatus - current states:', currentStates)
      }

      // 一括で状態更新（競合回避）
      if (updatesNeeded.length > 0) {
        console.log('[DEBUG] checkAllImagesStatus - updates needed:', updatesNeeded)
        isUpdatingStateRef.current = true

        // 必要な更新のみを含む新しいloading状態を返す
        const newLoading = { ...currentLoading }
        updatesNeeded.forEach(key => {
          newLoading[key] = false
        })

        // retryCountも更新
        setRetryCount(prev => {
          const newRetryCount = { ...prev }
          updatesNeeded.forEach(key => {
            newRetryCount[key] = 0
          })
          return newRetryCount
        })

        // 状態更新完了後にフラグをリセット
        setTimeout(() => {
          isUpdatingStateRef.current = false

          // 全ての画像が完了状態になったかチェック
          const allCompleted = graphs.every(graph => {
            const key = graph.endpoint
            const img = imageRefs.current[key]
            const isLoaded = img && img.complete && img.naturalWidth > 0
            const hasError = errors[key] && errors[key].length > 0
            return isLoaded || hasError
          })

          // 全て完了していたらインターバルを停止
          if (allCompleted && statusCheckIntervalRef.current) {
            clearInterval(statusCheckIntervalRef.current)
            statusCheckIntervalRef.current = null
          }
        }, 0)

        return newLoading
      }

      return currentLoading // 更新がない場合は現在の状態を返す
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

  const getImageUrl = useCallback((graph: GraphInfo, forceReload = false) => {
    // リトライ時は強制リロード、通常時は10分間キャッシュが効くようにする
    const now = new Date()
    let timestamp: number

    if (forceReload) {
      // リトライ時は現在時刻をそのまま使用してキャッシュを回避
      timestamp = now.getTime()
    } else {
      // 通常時は10分間隔のタイムスタンプでキャッシュ制御
      const tenMinutesInMs = 10 * 60 * 1000  // 10分をミリ秒で
      timestamp = Math.floor(now.getTime() / tenMinutesInMs) * tenMinutesInMs
    }

    const params = new URLSearchParams({
      start: formatDateForAPI(dateRange.start),
      end: formatDateForAPI(dateRange.end),
      _t: timestamp.toString()
    })
    return `${graph.endpoint}?${params}`
  }, [dateRange])

  // コンテナの幅を測定（レイアウト完了後）
  useLayoutEffect(() => {
    // 初回測定のために少し遅延させる
    setTimeout(() => {
      measureContainerWidths()
    }, 100)
  }, [])

  // ウィンドウリサイズ時にコンテナ幅を再測定
  useEffect(() => {
    const handleResize = () => {
      measureContainerWidths()
    }

    window.addEventListener('resize', handleResize)
    return () => window.removeEventListener('resize', handleResize)
  }, [])

  // ページ読み込み時にハッシュがあれば該当要素にスクロール
  useEffect(() => {
    if (window.location.hash === '#graph') {
      const element = document.getElementById('graph')
      if (element) {
        setTimeout(() => {
          element.scrollIntoView({ behavior: 'smooth', block: 'start' })
        }, 200)
      }
    }
  }, [])

  // 初回マウント時に画像URLを段階的に設定（ブラウザ同時接続制限対応）
  useEffect(() => {
    const loadImagesInBatches = async () => {
      const BATCH_SIZE = 4 // ブラウザの同時接続制限を考慮
      const BATCH_DELAY = 50 // バッチ間の遅延（ms）

      console.log('[Initial mount] Starting staged image loading for', graphs.length, 'images')

      for (let i = 0; i < graphs.length; i += BATCH_SIZE) {
        const batch = graphs.slice(i, i + BATCH_SIZE)
        const batchUrls: { [key: string]: string } = {}

        batch.forEach(graph => {
          const key = graph.endpoint
          batchUrls[key] = getImageUrl(graph)  // 通常のキャッシュ制御
        })

        console.log(`[Initial mount] Loading batch ${Math.floor(i/BATCH_SIZE) + 1}:`, Object.keys(batchUrls))
        setImageUrls(prev => ({ ...prev, ...batchUrls }))

        // 最後のバッチでない場合は遅延
        if (i + BATCH_SIZE < graphs.length) {
          await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
        }
      }
    }

    loadImagesInBatches()
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

  // 画像の読み込み状態を管理（排他制御付き）
  const handleImageLoad = useCallback((key: string) => {
    console.log('[DEBUG] handleImageLoad called for', key)
    // 状態更新中は重複実行を防ぐ
    if (isUpdatingStateRef.current) {
      console.log('[DEBUG] handleImageLoad skipped (updating state) for', key)
      return
    }

    const img = imageRefs.current[key]
    console.log('[DEBUG] handleImageLoad - image state:', {
      key,
      imgExists: !!img,
      complete: img?.complete,
      naturalWidth: img?.naturalWidth,
      src: img?.src?.substring(0, 50) + '...'
    })

    if (img && img.complete && img.naturalWidth > 0) {
      console.log('[DEBUG] handleImageLoad - image is valid, updating state for', key)
      isUpdatingStateRef.current = true

      // タイマーをクリア
      if (loadingTimers[key]) {
        clearTimeout(loadingTimers[key])
        setLoadingTimers(prev => {
          const newTimers = { ...prev }
          delete newTimers[key]
          return newTimers
        })
      }

      // バッチ処理で一度に状態を更新
      console.log('[DEBUG] handleImageLoad - setting loading to false for', key)
      setLoading(prev => ({ ...prev, [key]: false }))
      setRetryCount(prev => ({ ...prev, [key]: 0 }))

      // 状態更新完了後にフラグをリセット
      setTimeout(() => {
        console.log('[DEBUG] handleImageLoad - state update completed for', key)
        isUpdatingStateRef.current = false
      }, 0)
    } else {
      console.log(`[DEBUG] handleImageLoad - ${key}: invalid image state, not marking as loaded`)
    }
  }, [loadingTimers])

  const handleImageError = useCallback((key: string, title: string) => {
    console.log('[DEBUG] handleImageError called for', key, title)
    // 状態更新中は重複実行を防ぐ
    if (isUpdatingStateRef.current) {
      console.log('[DEBUG] handleImageError skipped (updating state) for', key)
      return
    }

    const img = imageRefs.current[key]
    console.log(`[DEBUG] handleImageError - ${key}: image error occurred`)
    console.log(`[DEBUG] handleImageError - ${key}: title = ${title}`)
    console.log(`[DEBUG] handleImageError - ${key}: img.src = ${img?.src}`)
    console.log(`[DEBUG] handleImageError - ${key}: img.complete = ${img?.complete}`)
    console.log(`[DEBUG] handleImageError - ${key}: img.naturalWidth = ${img?.naturalWidth}`)

    isUpdatingStateRef.current = true

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

    // 状態更新完了後にフラグをリセット
    setTimeout(() => {
      isUpdatingStateRef.current = false
    }, 0)
  }, [loadingTimers])

  // 画像の再読み込みを行う
  const retryImageLoad = useCallback((key: string, currentRetryCount?: number) => {
    const actualRetryCount = currentRetryCount !== undefined ? currentRetryCount : (retryCount[key] || 0)
    console.log(`[retryImageLoad] ${key}: retry count = ${actualRetryCount}`)

    // 最大2回までリトライ（計3回試行：初回 + リトライ2回）
    if (actualRetryCount < MAX_RETRY_COUNT) {

      // リトライ回数を更新
      const nextRetryCount = actualRetryCount + 1
      setRetryCount(prev => ({ ...prev, [key]: nextRetryCount }))

      // 該当する画像のURLを強制リロードで更新
      const graph = graphs.find(g => g.endpoint === key)
      if (graph) {
        const newUrl = getImageUrl(graph, true)  // forceReload = true
        console.log(`[retryImageLoad] ${key}: setting new URL = ${newUrl}`)
        setImageUrls(prev => ({ ...prev, [key]: newUrl }))
        setLoading(prev => ({ ...prev, [key]: true }))
        setErrors(prev => ({ ...prev, [key]: '' }))

        // 新しいタイムアウトタイマーを設定
        const newTimer = window.setTimeout(() => {
          const img = imageRefs.current[key]
          console.log(`[Timeout check] ${key}: img exists=${!!img}, complete=${img?.complete}, naturalWidth=${img?.naturalWidth}`)

          if (!img || !img.src) {
            console.log(`[Timeout check] ${key}: Image element or src missing`)
            retryImageLoad(key, nextRetryCount)
          } else if (img.complete && img.naturalWidth > 0) {
            console.log(`[Timeout check] ${key}: Image loaded successfully`)
            handleImageLoad(key)
          } else if (img.complete && img.naturalWidth === 0) {
            console.log(`[Timeout check] ${key}: Error state detected`)
            handleImageError(key, graph.title)
          } else {
            console.log(`[Timeout check] ${key}: Still loading, continuing retry`)
            retryImageLoad(key, nextRetryCount)
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
  }, [retryCount, getImageUrl, handleImageLoad, handleImageError])

  // Memoized callback for reload button
  const handleReload = useCallback((key: string) => {
    setErrors(prev => ({ ...prev, [key]: '' }))
    setRetryCount(prev => ({ ...prev, [key]: 0 }))
    setLoading(prev => ({ ...prev, [key]: true }))

    // Force reload with new URL
    const graph = graphs.find(g => g.endpoint === key)
    if (graph) {
      setImageUrls(prev => ({ ...prev, [key]: getImageUrl(graph, true) }))
    }
  }, [getImageUrl])

  useEffect(() => {
    console.log('[DEBUG] dateRange useEffect triggered:', {
      start: dateRange.start.toISOString(),
      end: dateRange.end.toISOString(),
      timestamp: new Date().toISOString()
    })

    // 既存のタイマーをクリア
    setLoadingTimers(prev => {
      console.log('[DEBUG] Clearing existing timers:', Object.keys(prev))
      Object.values(prev).forEach(timer => clearTimeout(timer))
      return {}
    })

    // 状態更新を同期的に処理するため、一度の更新にまとめる
    const newImageUrls: { [key: string]: string } = {}
    const newLoadingState: { [key: string]: boolean } = {}
    const newErrorState: { [key: string]: string } = {}
    const newTimers: { [key: string]: number } = {}

    console.log('[DEBUG] Generating new image URLs for graphs:', graphs.length)

    graphs.forEach((graph) => {
      const key = graph.endpoint
      const imageUrl = getImageUrl(graph)  // 通常のキャッシュ制御
      newImageUrls[key] = imageUrl
      newLoadingState[key] = true
      newErrorState[key] = ''

      console.log('[DEBUG] Generated URL for', key, ':', imageUrl)

      // 各画像に対してタイムアウトタイマーを設定（画像要素の実際の状態をチェック）
      newTimers[key] = window.setTimeout(() => {
        const img = imageRefs.current[key]
        console.log('[DEBUG] Timeout check for', key, ':', {
          imgExists: !!img,
          src: img?.src?.substring(0, 50) + '...',
          complete: img?.complete,
          naturalWidth: img?.naturalWidth,
          naturalHeight: img?.naturalHeight
        })

        if (!img || !img.src) {
          console.log('[DEBUG] Image element missing or no src for', key)
          retryImageLoad(key, 0)
        } else if (img.complete && img.naturalWidth > 0) {
          console.log('[DEBUG] Image loaded successfully in timeout check for', key)
          // 実際には読み込み完了していた場合
          handleImageLoad(key)
        } else if (img.complete && img.naturalWidth === 0) {
          console.log('[DEBUG] Image error detected in timeout check for', key)
          // エラー状態の検出
          handleImageError(key, graph.title)
        } else {
          console.log('[DEBUG] Image still loading in timeout check for', key)
          // まだ読み込み中
          retryImageLoad(key, 0)
        }
      }, IMAGE_LOAD_TIMEOUT)
    })

    // 状態を一括更新
    console.log('[DEBUG] Updating states - loading:', Object.keys(newLoadingState))
    setLoading(newLoadingState)
    setErrors(newErrorState)
    setRetryCount({})
    setLoadingTimers(newTimers)
    console.log('[DEBUG] States updated - timers set:', Object.keys(newTimers))

    // 段階的設定（ブラウザ同時接続制限対応）
    const setBatchUrls = async () => {
      const BATCH_SIZE = 4
      const BATCH_DELAY = 100

      console.log('[Date change] Starting staged image loading for', Object.keys(newImageUrls).length, 'images')

      const urlEntries = Object.entries(newImageUrls)
      for (let i = 0; i < urlEntries.length; i += BATCH_SIZE) {
        const batch = urlEntries.slice(i, i + BATCH_SIZE)
        const batchUrls = Object.fromEntries(batch)

        console.log(`[Date change] Loading batch ${Math.floor(i/BATCH_SIZE) + 1}:`, Object.keys(batchUrls))
        console.log('[DEBUG] Setting batch URLs:', batchUrls)
        setImageUrls(prev => {
          const newUrls = { ...prev, ...batchUrls }
          console.log('[DEBUG] Updated imageUrls state:', Object.keys(newUrls))
          return newUrls
        })

        if (i + BATCH_SIZE < urlEntries.length) {
          await new Promise(resolve => setTimeout(resolve, BATCH_DELAY))
        }
      }
    }

    setBatchUrls()

    // 既存のインターバルをクリア
    if (statusCheckIntervalRef.current) {
      clearInterval(statusCheckIntervalRef.current)
    }

    // 定期的な画像状態チェックを開始（onLoadイベント消失を補完）
    statusCheckIntervalRef.current = setInterval(() => {
      checkAllImagesStatus()
    }, 100) // 100msごとにチェック（応答性向上）

    // クリーンアップ関数
    return () => {
      Object.values(newTimers).forEach(timer => clearTimeout(timer))
      if (statusCheckIntervalRef.current) {
        clearInterval(statusCheckIntervalRef.current)
        statusCheckIntervalRef.current = null
      }
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

          // 実際のコンテナ幅から画像がロードされた時と同じ高さを計算
          const is3D = graph.endpoint.includes('3d')
          const actualContainerWidth = containerWidths[key]

          let calculatedHeight: number
          if (actualContainerWidth) {
            // 実際の表示幅が測定済みの場合は、正確な高さを計算
            calculatedHeight = calculateActualHeight(graph, actualContainerWidth)
          } else {
            // まだ測定されていない場合はフォールバック値を使用
            const [width, height] = graph.size
            const aspectRatio = height / width
            const estimatedWidth = is3D ? 600 : 350
            calculatedHeight = estimatedWidth * aspectRatio
          }

          // 計算された高さをpxで設定（card全体の一貫した高さを確保）
          const containerHeight = calculatedHeight + 'px'
          const cardPadding = 16  // 0.5rem * 2 (上下) = 16px
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
                    height: containerHeight,  // 固定高さ
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    position: 'relative',
                    overflow: 'hidden',  // はみ出した部分を隠す
                    flex: '1 1 auto'  // flexboxで残りのスペースを占有
                  }}>
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
                          ref={(el) => {
                            if (el && imageRefs.current[key] !== el) {
                              console.log('[DEBUG] Setting image ref for', key, !!el)
                              imageRefs.current[key] = el
                            } else if (!el && imageRefs.current[key]) {
                              console.log('[DEBUG] Clearing image ref for', key)
                              imageRefs.current[key] = null
                            }
                          }}
                          src={imageUrl}
                          alt={graph.title}
                          style={{
                            width: '100%',
                            height: '100%',
                            objectFit: 'contain',  // アスペクト比を維持しながら収める
                            cursor: 'pointer'
                          }}
                          onClick={() => onImageClick(imageUrl)}
                          onLoad={() => {
                            console.log('[DEBUG] onLoad event fired for', key)
                            handleImageLoad(key)
                          }}
                          onError={() => {
                            console.log('[DEBUG] onError event fired for', key)
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
