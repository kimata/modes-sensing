import { useState, useEffect, useCallback } from 'react'
import { PaperAirplaneIcon } from '@heroicons/react/24/solid'
import DateSelector from './components/DateSelector'
import GraphDisplay from './components/GraphDisplay'
import Modal from './components/Modal'
import Footer from './components/Footer'
import ReceiverStatus from './components/ReceiverStatus'
import { parseUrlParams, updateUrl, resetUrl, PERIOD_DAYS, type PeriodType } from './hooks/useUrlParams'
import type { DataRangeResponse } from './types/api'

function App() {
  // URL パラメータを解析
  const urlParams = parseUrlParams()

  // 初期期間タイプを決定
  const getInitialPeriod = (): PeriodType => {
    if (urlParams.hasUrlParams && urlParams.period) {
      return urlParams.period
    }
    return '7days' // デフォルト
  }

  // 初期日付範囲を決定
  const getInitialDate = () => {
    // カスタム期間でstart/endが指定されている場合
    if (urlParams.hasUrlParams && urlParams.period === 'custom' && urlParams.start && urlParams.end) {
      return { start: urlParams.start, end: urlParams.end }
    }

    // 期間タイプから計算（URL パラメータまたはデフォルト）
    const period = getInitialPeriod()
    const days = period === 'custom' ? 7 : PERIOD_DAYS[period]

    const end = new Date()
    end.setSeconds(0, 0)
    const start = new Date(Date.now() - days * 24 * 60 * 60 * 1000)
    start.setSeconds(0, 0)
    return { start, end }
  }

  const getInitialLimitAltitude = () => {
    if (urlParams.hasUrlParams) {
      return urlParams.limitAltitude
    }
    return false
  }

  const [dateRange, setDateRange] = useState(getInitialDate())
  const [selectedPeriod, setSelectedPeriod] = useState<PeriodType>(getInitialPeriod())
  // URL パラメータがある場合は、data-range による調整をスキップ
  const [isInitialDateRangeSet, setIsInitialDateRangeSet] = useState(urlParams.hasUrlParams)
  const [modalImage, setModalImage] = useState<string | null>(null)
  const [dataRange, setDataRange] = useState<DataRangeResponse | null>(null)
  const [dataRangeSubtitle, setDataRangeSubtitle] = useState<string>('')
  const [limitAltitude, setLimitAltitude] = useState(getInitialLimitAltitude)

  // データ範囲を取得し、初期日付範囲を調整
  useEffect(() => {
    const fetchDataRange = async () => {
      try {
        const response = await fetch('/modes-sensing/api/data-range')
        if (response.ok) {
          const range: DataRangeResponse = await response.json()
          setDataRange(range)

          // 初期日付範囲をデータ範囲に基づいて調整
          if (range.earliest && range.latest && !isInitialDateRangeSet) {
            const currentRange = getInitialDate()
            const dataEarliest = new Date(range.earliest)
            const dataLatest = new Date(range.latest)

            let adjustedStart = new Date(currentRange.start)
            let adjustedEnd = new Date(currentRange.end)
            let needsAdjustment = false


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
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isInitialDateRangeSet])

  // 期間変更ハンドラ（クイック選択・カスタム両対応）
  const handlePeriodChange = useCallback((period: PeriodType, start: Date, end: Date) => {
    setDateRange({ start, end })
    setSelectedPeriod(period)
    // URL を更新
    updateUrl(period, start, end, limitAltitude)
  }, [limitAltitude])

  const handleAltitudeChange = useCallback((limited: boolean) => {
    setLimitAltitude(limited)
    // URL を更新
    updateUrl(selectedPeriod, dateRange.start, dateRange.end, limited)
  }, [selectedPeriod, dateRange])

  // タイトルクリック: デフォルト状態にリセット
  const handleTitleClick = useCallback(() => {
    const end = new Date()
    end.setSeconds(0, 0)
    const start = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)
    start.setSeconds(0, 0)

    setDateRange({ start, end })
    setSelectedPeriod('7days')
    setLimitAltitude(false)
    resetUrl()
  }, [])

  const handleImageClick = (imageUrl: string) => {
    setModalImage(imageUrl)
  }

  const handleModalClose = () => {
    setModalImage(null)
  }

  return (
    <div className="max-w-screen-xl mx-auto px-4">
      <section className="py-12 px-6">
        <div className="max-w-screen-xl mx-auto">
          <h1 className="text-4xl font-semibold text-center mb-4">
            <a
              href={window.location.pathname}
              onClick={(e) => {
                e.preventDefault()
                handleTitleClick()
              }}
              className="text-inherit no-underline cursor-pointer transition-opacity hover:opacity-70"
              title="クリックでデフォルト表示に戻る"
            >
              <PaperAirplaneIcon className="w-10 h-10 mr-2 inline-block -rotate-45" />
              航空機の気象データ
            </a>
          </h1>

          {dataRangeSubtitle && (
            <p className="text-base text-gray-600 text-center mt-2 mb-8 flex flex-wrap justify-center gap-1 items-baseline">
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
            initialPeriod={urlParams.hasUrlParams ? urlParams.period : null}
            onPeriodChange={handlePeriodChange}
            dataRange={dataRange}
            limitAltitude={limitAltitude}
            onAltitudeChange={handleAltitudeChange}
          />

          {isInitialDateRangeSet ? (
            <>
              <GraphDisplay
                dateRange={dateRange}
                limitAltitude={limitAltitude}
                onImageClick={handleImageClick}
              />
              <ReceiverStatus />
            </>
          ) : (
            <div className="mb-5">
              <div className="text-center">
                <div className="loader mx-auto"></div>
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
