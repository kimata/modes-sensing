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
  const [isInitialDateRangeSet, setIsInitialDateRangeSet] = useState(false)
  const [modalImage, setModalImage] = useState<string | null>(null)
  const [dataRange, setDataRange] = useState<DataRange | null>(null)
  const [dataRangeSubtitle, setDataRangeSubtitle] = useState<string>('')

  // データ範囲を取得し、初期日付範囲を調整
  useEffect(() => {
    const fetchDataRange = async () => {
      try {
        const response = await fetch('/modes-sensing/api/data-range')
        if (response.ok) {
          const range: DataRange = await response.json()
          setDataRange(range)

          // 初期日付範囲をデータ範囲に基づいて調整
          if (range.earliest && range.latest && !isInitialDateRangeSet) {
            const currentRange = getInitialDate()
            const dataEarliest = new Date(range.earliest)
            const dataLatest = new Date(range.latest)

            let adjustedStart = new Date(currentRange.start)
            let adjustedEnd = new Date(currentRange.end)
            let needsAdjustment = false

            console.log('[App] Initial date range adjustment check:', {
              currentStart: adjustedStart.toISOString(),
              currentEnd: adjustedEnd.toISOString(),
              dataEarliest: dataEarliest.toISOString(),
              dataLatest: dataLatest.toISOString()
            })

            // 終了日時が利用可能なデータの最新日時を超えている場合
            if (adjustedEnd > dataLatest) {
              adjustedEnd = new Date(dataLatest)
              adjustedEnd.setSeconds(0, 0)
              needsAdjustment = true
            }

            // 開始日時が利用可能なデータの最古日時を下回っている場合
            if (adjustedStart < dataEarliest) {
              adjustedStart = new Date(dataEarliest)
              adjustedStart.setSeconds(0, 0)
              needsAdjustment = true
            }

            // 7日間の期間を維持しようとする
            const sevenDaysMs = 7 * 24 * 60 * 60 * 1000
            const recalculatedStart = new Date(adjustedEnd.getTime() - sevenDaysMs)
            if (recalculatedStart >= dataEarliest) {
              adjustedStart = recalculatedStart
              adjustedStart.setSeconds(0, 0)
              needsAdjustment = true
            }

            if (needsAdjustment) {
              console.log('[App] Adjusting initial date range:', {
                from: { start: currentRange.start.toISOString(), end: currentRange.end.toISOString() },
                to: { start: adjustedStart.toISOString(), end: adjustedEnd.toISOString() }
              })
              setDateRange({ start: adjustedStart, end: adjustedEnd })
            }

            setIsInitialDateRangeSet(true)
          }

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

            setDataRangeSubtitle(`過去${daysFormatted}日間（${startDateFormatted}〜）、計 ${countFormatted} 件のデータが記録されています`)
          }
        } else {
          // データ範囲の取得に失敗した場合も初期化を完了させる
          setIsInitialDateRangeSet(true)
        }
      } catch (error) {
        console.error('データ範囲の取得に失敗しました:', error)
        // エラー時も初期化を完了させる
        setIsInitialDateRangeSet(true)
      }
    }

    fetchDataRange()
  }, [isInitialDateRangeSet])

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
                // 「、」と「が」で分割して改行可能なセグメントを作成
                const text = dataRangeSubtitle
                const segments = []

                // まず「が」で分割
                const mainParts = text.split('が')
                const beforeGa = mainParts[0] // 「〜が」より前
                const afterGa = mainParts[1] // 「が」より後

                // 「が」より前の部分を「、」でさらに分割
                const beforeGaParts = beforeGa.split('、')

                // 最初の部分: 「過去4日間（2025年08月05日〜）」
                if (beforeGaParts.length > 0) {
                  segments.push(
                    <span key="part1" style={{ whiteSpace: 'nowrap' }}>
                      {beforeGaParts[0]}、
                    </span>
                  )
                }

                // 真ん中の部分: 「計 85,090 件のデータ」
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

          {isInitialDateRangeSet ? (
            <GraphDisplay
              dateRange={dateRange}
              onImageClick={handleImageClick}
            />
          ) : (
            <div className="box">
              <div className="has-text-centered">
                <div className="loader"></div>
                <p className="mt-2">データ範囲を確認中...</p>
              </div>
            </div>
          )}
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
