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

            // 件数をカンマ区切りでフォーマット
            const countFormatted = range.count ? range.count.toLocaleString('ja-JP') : '0'
            const daysFormatted = daysDiff.toLocaleString('ja-JP')

            setDataRangeSubtitle(`過去${daysFormatted}日間（${startDateFormatted}〜）のデータ${countFormatted}件が記録されています`)
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
            <p className="subtitle is-6 has-text-centered" style={{
              marginTop: '0.5rem',
              marginBottom: '2rem',
              display: 'flex',
              flexWrap: 'wrap',
              justifyContent: 'center',
              gap: '0.25rem',
              alignItems: 'baseline'
            }}>
              {/* 複数の改行ポイントで文字列を分割 */}
              {(() => {
                // 「の」と「が」で分割して改行可能なセグメントを作成
                const text = dataRangeSubtitle
                const segments = []

                // まず「が」で分割
                const mainParts = text.split('が')
                const beforeGa = mainParts[0] // 「〜が」より前
                const afterGa = mainParts[1] // 「が」より後

                // 「が」より前の部分を「の」でさらに分割
                const beforeGaParts = beforeGa.split('の')

                // 最初の部分: 「過去4日間（2025年08月05日〜）」
                if (beforeGaParts.length > 0) {
                  segments.push(
                    <span key="part1" style={{ whiteSpace: 'nowrap' }}>
                      {beforeGaParts[0]}の
                    </span>
                  )
                }

                // 真ん中の部分: 「データ85,090件」
                if (beforeGaParts.length > 1) {
                  segments.push(
                    <span key="part2" style={{ whiteSpace: 'nowrap' }}>
                      {beforeGaParts[1]}が
                    </span>
                  )
                }

                // 最後の部分: 「記録されています」
                if (afterGa) {
                  segments.push(
                    <span key="part3" style={{ whiteSpace: 'nowrap' }}>
                      {afterGa}
                    </span>
                  )
                }

                return segments
              })()}
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
