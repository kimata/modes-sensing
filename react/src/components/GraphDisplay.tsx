import { useState, useEffect } from 'react'

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

  const getImageUrl = (graph: GraphInfo) => {
    const params = new URLSearchParams({
      start: formatDateForAPI(dateRange.start),
      end: formatDateForAPI(dateRange.end)
    })
    return `${graph.endpoint}?${params}`
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
      console.log(`Retrying image load for ${key} (attempt ${currentRetryCount + 1}/2)`)
      setRetryCount(prev => ({ ...prev, [key]: currentRetryCount + 1 }))
      setImageVersion(prev => prev + 1) // バージョンを更新して再読み込みを強制
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

    graphs.forEach(graph => {
      const key = graph.endpoint
      newImageUrls[key] = getImageUrl(graph)
      newLoadingState[key] = true  // 初期状態は読み込み中
      newErrorState[key] = ''
    })

    setImageUrls(newImageUrls)
    setLoading(newLoadingState)
    setErrors(newErrorState)
    setRetryCount({}) // リトライ回数をリセット
    // バージョンを更新して画像の再読み込みを促す
    setImageVersion(prev => prev + 1)

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
      if (loading[key]) {
        // 10秒のタイムアウトを設定
        newTimers[key] = window.setTimeout(() => {
          console.log(`Image loading timeout for ${key}`)
          retryImageLoad(key)
        }, 10000)
      }
    })

    setLoadingTimers(prev => ({ ...prev, ...newTimers }))

    // クリーンアップ関数
    return () => {
      Object.values(newTimers).forEach(timer => clearTimeout(timer))
    }
  }, [loading, imageVersion])

  return (
    <div className="box">
      <h2 className="title is-4">
        <span className="icon" style={{ marginRight: '0.5em' }}>
          <i className="fas fa-chart-line"></i>
        </span>
        グラフ
        <span className="subtitle is-6 ml-2">
          ({formatDateForDisplay(dateRange.start)} ～ {formatDateForDisplay(dateRange.end)})
        </span>
      </h2>

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
                        {error}
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
  )
}

export default GraphDisplay
