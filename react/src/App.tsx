import { useState, useEffect } from 'react'
import DateSelector from './components/DateSelector'
import GraphDisplay from './components/GraphDisplay'
import Modal from './components/Modal'
import Footer from './components/Footer'

interface DataRange {
  earliest: string | null
  latest: string | null
  count?: number
}

function App() {
  const getInitialDate = () => {
    const end = new Date()
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定
    const start = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) // 7 days ago
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    return { start, end }
  }

  const [dateRange, setDateRange] = useState(getInitialDate())
  const [modalImage, setModalImage] = useState<string | null>(null)
  const [dataRange, setDataRange] = useState<DataRange | null>(null)
  const [dataRangeSubtitle, setDataRangeSubtitle] = useState<string>('')

  // データ範囲を取得
  useEffect(() => {
    const fetchDataRange = async () => {
      try {
        const response = await fetch('/modes-sensing/api/data-range')
        if (response.ok) {
          const range: DataRange = await response.json()
          setDataRange(range)

          // サブタイトルを生成（参考ファイルのフォーマットに従う）
          if (range.earliest && range.latest) {
            const earliest = new Date(range.earliest)
            const latest = new Date(range.latest)

            // 日数を計算
            const daysDiff = Math.floor((latest.getTime() - earliest.getTime()) / (24 * 60 * 60 * 1000)) + 1

            // 開始日をフォーマット（年月日のみ）
            const startDateFormatted = earliest.toLocaleDateString('ja-JP', {
              year: 'numeric',
              month: '2-digit',
              day: '2-digit'
            }).replace(/\//g, '年').replace(/年(\d+)年/, '年$1月') + '日'

            setDataRangeSubtitle(`過去${daysDiff}日間（${startDateFormatted}〜）のデータが記録されています`)
          }
        }
      } catch (error) {
        console.error('データ範囲の取得に失敗しました:', error)
      }
    }

    fetchDataRange()
  }, [])

  const handleDateChange = (start: Date, end: Date) => {
    setDateRange({ start, end })
  }

  const handleImageClick = (imageUrl: string) => {
    setModalImage(imageUrl)
  }

  const handleModalClose = () => {
    setModalImage(null)
  }

  return (
    <div className="container">
      <section className="section">
        <div className="container">
          <h1 className="title is-2 has-text-centered">
            <span className="icon is-large" style={{ marginRight: '0.5em' }}>
              <i className="fas fa-plane"></i>
            </span>
            航空機の気象データ
            <span style={{ marginLeft: '0.5em' }}></span>
          </h1>

          {dataRangeSubtitle && (
            <p className="subtitle is-6 has-text-centered" style={{ marginTop: '-0.5rem', marginBottom: '2rem' }}>
              {dataRangeSubtitle}
            </p>
          )}

          <DateSelector
            startDate={dateRange.start}
            endDate={dateRange.end}
            onDateChange={handleDateChange}
            dataRange={dataRange}
          />

          <GraphDisplay
            dateRange={dateRange}
            onImageClick={handleImageClick}
          />
        </div>
      </section>

      {modalImage && (
        <Modal
          imageUrl={modalImage}
          onClose={handleModalClose}
        />
      )}
      <Footer />
    </div>
  )
}

export default App
