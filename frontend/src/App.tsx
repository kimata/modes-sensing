import { useState, useEffect, useCallback } from 'react'
import DateSelector from './components/DateSelector'
import GraphDisplay from './components/GraphDisplay'
import Modal from './components/Modal'
import Footer from './components/Footer'
import ReceiverStatus from './components/ReceiverStatus'
import { parseUrlParams, updateUrl, resetUrl, PERIOD_DAYS, type PeriodType } from './hooks/useUrlParams'
import { clampRangeToData } from './utils/dateRange'
import type { DataRangeResponse } from './types/api'

// サブタイトル用の構造化データ
interface DataSummary {
  days: number
  startDate: Date
  count: number
}

// 日本語の年月日フォーマット（例: "2025年08月05日"）
const formatJaDate = (date: Date): string => {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}年${month}月${day}日`
}

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
  const [dataSummary, setDataSummary] = useState<DataSummary | null>(null)
  const [limitAltitude, setLimitAltitude] = useState(getInitialLimitAltitude)

  // データ範囲を取得し、初期日付範囲を調整（初回マウント時のみ実行）
  useEffect(() => {
    let cancelled = false

    const fetchDataRange = async () => {
      try {
        const response = await fetch('/modes-sensing/api/data-range')
        if (cancelled) return

        if (response.ok) {
          const range: DataRangeResponse = await response.json()
          if (cancelled) return

          setDataRange(range)

          // 初期日付範囲をデータ範囲に基づいて調整
          // （URL パラメータで明示指定されている場合はスキップ）
          if (range.earliest && range.latest && !urlParams.hasUrlParams) {
            const currentRange = getInitialDate()
            const clamped = clampRangeToData(currentRange.start, currentRange.end, range, {
              preservePeriodMs: 7 * 24 * 60 * 60 * 1000
            })

            if (clamped.adjusted) {
              setDateRange({ start: clamped.start, end: clamped.end })
            }
          }

          // サブタイトル用の構造化データを設定
          if (range.earliest && range.latest) {
            const earliest = new Date(range.earliest)
            const latest = new Date(range.latest)
            const daysDiff = Math.floor((latest.getTime() - earliest.getTime()) / (24 * 60 * 60 * 1000)) + 1

            setDataSummary({
              days: daysDiff,
              startDate: earliest,
              count: range.count ?? 0
            })
          }
        }
      } catch (error) {
        console.error('データ範囲の取得に失敗しました:', error)
      } finally {
        // 成功・失敗にかかわらず初期化を完了させる
        if (!cancelled) {
          setIsInitialDateRangeSet(true)
        }
      }
    }

    fetchDataRange()

    return () => {
      cancelled = true
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

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
              <img src="/modes-sensing/airplane.svg" alt="airplane" className="w-[60px] h-[60px] mr-2 inline-block" />
              航空機の気象データ
            </a>
          </h1>

          {dataSummary && (
            <p className="text-base text-gray-600 text-center mt-2 mb-8 flex flex-wrap justify-center gap-1 items-baseline">
              <span style={{ whiteSpace: 'nowrap' }}>
                過去{dataSummary.days.toLocaleString('ja-JP')}日間（{formatJaDate(dataSummary.startDate)}〜）、
              </span>
              <span style={{ whiteSpace: 'nowrap' }}>
                計 {dataSummary.count.toLocaleString('ja-JP')} 件のデータが
              </span>
              <span style={{ whiteSpace: 'nowrap' }}>
                記録されています
              </span>
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
