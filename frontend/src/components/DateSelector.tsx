import { useState, useEffect, useRef } from 'react'
import { CalendarDaysIcon, LinkIcon, ArrowPathIcon, CheckIcon } from '@heroicons/react/24/outline'
import styles from './GraphDisplay.module.css'
import { PERIOD_DAYS, type PeriodType } from '../hooks/useUrlParams'
import { usePermalink } from '../hooks/usePermalink'
import { formatDateForInput } from '../utils/date'
import { clampRangeToData } from '../utils/dateRange'
import type { DataRangeResponse } from '../types/api'

// 山アイコン（Heroicons にないためカスタム SVG）
const MountainIcon: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" d="M3 19.5l6.75-9 4.5 6 3-4.5L21 19.5H3z" />
  </svg>
)

interface DateSelectorProps {
  startDate: Date
  endDate: Date
  initialPeriod: PeriodType | null  // URL から渡される初期期間
  onPeriodChange: (period: PeriodType, start: Date, end: Date) => void
  dataRange: DataRangeResponse | null
  limitAltitude: boolean
  onAltitudeChange: (limited: boolean) => void
}

const DateSelector: React.FC<DateSelectorProps> = ({
  startDate,
  endDate,
  initialPeriod,
  onPeriodChange,
  dataRange,
  limitAltitude,
  onAltitudeChange
}) => {
  const [customStart, setCustomStart] = useState(formatDateForInput(startDate))
  const [customEnd, setCustomEnd] = useState(formatDateForInput(endDate))
  const [hasChanges, setHasChanges] = useState(false)
  const [focusedField, setFocusedField] = useState<'start' | 'end' | null>(null)

  // 初期期間を設定（URL パラメータから、またはデフォルトで7days）
  // ユーザーが明示的に選択した表示モードを維持する（期間からの自動判定は行わない）
  const [selectedPeriod, setSelectedPeriod] = useState<PeriodType>(initialPeriod || '7days')

  // 開始日時入力フィールドへの参照（カスタムボタンクリック時のフォーカス用）
  const startInputRef = useRef<HTMLInputElement>(null)

  // パーマリンクコピー機能（共通フック）
  const { notificationRef, notificationClassName, copyPermalink } = usePermalink()

  // propsが変更されたときに入力フィールドを更新
  useEffect(() => {
    setCustomStart(formatDateForInput(startDate))
    setCustomEnd(formatDateForInput(endDate))
    setHasChanges(false)
  }, [startDate, endDate])

  // 日付入力が変更されたかチェック
  useEffect(() => {
    const currentStartStr = formatDateForInput(startDate)
    const currentEndStr = formatDateForInput(endDate)
    setHasChanges(customStart !== currentStartStr || customEnd !== currentEndStr)
  }, [customStart, customEnd, startDate, endDate])

  // ページ読み込み時にハッシュがあれば該当要素にスクロール
  useEffect(() => {
    if (window.location.hash === '#date-selector') {
      const element = document.getElementById('date-selector')
      if (element) {
        setTimeout(() => {
          element.scrollIntoView({ behavior: 'smooth', block: 'start' })
        }, 500)
      }
    }
  }, [])

  const handleQuickSelect = (days: number, period: Exclude<PeriodType, 'custom'>) => {
    const end = new Date()
    const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000)

    // データ範囲による調整（要求期間の維持を試みる）
    const clamped = clampRangeToData(start, end, dataRange, {
      preservePeriodMs: days * 24 * 60 * 60 * 1000
    })

    setSelectedPeriod(period)

    onPeriodChange(period, clamped.start, clamped.end)
    setCustomStart(formatDateForInput(clamped.start))
    setCustomEnd(formatDateForInput(clamped.end))
  }

  const handleCustomDateChange = () => {
    const start = new Date(customStart)
    const end = new Date(customEnd)
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定

    // 基本的な日付順序チェック
    if (start > end) {
      alert('開始日時は終了日時より前に設定してください')
      return
    }

    // データ範囲による自動調整（成立しない場合は alert 表示）
    const clamped = clampRangeToData(start, end, dataRange, { showAlert: true })
    if (!clamped.valid) {
      return
    }

    onPeriodChange('custom', clamped.start, clamped.end)
    setHasChanges(false)
    setSelectedPeriod('custom')

    // 調整された場合は入力フィールドも更新
    if (clamped.adjusted) {
      setCustomStart(formatDateForInput(clamped.start))
      setCustomEnd(formatDateForInput(clamped.end))
    }
  }

  const handleCustomButtonClick = () => {
    // 直前に選択されていた期間ボタンに基づいて日時を設定
    if (selectedPeriod !== 'custom') {
      const days = PERIOD_DAYS[selectedPeriod]
      if (days) {
        const end = new Date()
        const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000)

        // データ範囲による調整（要求期間の維持を試みる）
        const clamped = clampRangeToData(start, end, dataRange, {
          preservePeriodMs: days * 24 * 60 * 60 * 1000
        })

        setCustomStart(formatDateForInput(clamped.start))
        setCustomEnd(formatDateForInput(clamped.end))
      }
    }

    setSelectedPeriod('custom')

    // カスタムボタンクリック時は入力フィールドにフォーカス
    setTimeout(() => {
      startInputRef.current?.focus()
    }, 100)
  }

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && hasChanges) {
      handleCustomDateChange()
    }
  }

  // フォーカス管理
  const handleInputFocus = (field: 'start' | 'end') => {
    setFocusedField(field)
  }

  const handleInputBlur = () => {
    setFocusedField(null)
  }

  // データ範囲に基づいて入力フィールドのmin/maxを設定
  const getInputLimits = () => {
    if (dataRange && dataRange.earliest && dataRange.latest) {
      const earliest = new Date(dataRange.earliest)
      const latest = new Date(dataRange.latest)
      return {
        min: formatDateForInput(earliest),
        max: formatDateForInput(latest)
      }
    }
    return { min: undefined, max: undefined }
  }

  const inputLimits = getInputLimits()

  return (
    <>
      <div className="mb-5" id="date-selector">
        <div className={styles.sectionHeader}>
          <h2 className="text-2xl font-semibold whitespace-nowrap">
            <CalendarDaysIcon className="w-6 h-6 inline-block mr-2" />
            期間選択
            <LinkIcon
              className={`w-4 h-4 inline-block ${styles.permalinkIcon}`}
              onClick={() => copyPermalink('date-selector')}
              title="パーマリンクをコピー"
            />
          </h2>
        </div>

      <div className="mb-4">
        <label className="block text-sm font-medium text-gray-700 mb-1">クイック選択</label>
        <div className="flex flex-wrap gap-2">
            <button
              className={`btn btn-sm whitespace-nowrap ${selectedPeriod === '1day' ? 'btn-primary' : 'btn-light'}`}
              onClick={() => handleQuickSelect(1, '1day')}
            >
              過去24時間
            </button>
            <button
              className={`btn btn-sm whitespace-nowrap ${selectedPeriod === '7days' ? 'btn-primary' : 'btn-light'}`}
              onClick={() => handleQuickSelect(7, '7days')}
            >
              過去7日間
            </button>
            <button
              className={`btn btn-sm whitespace-nowrap ${selectedPeriod === '30days' ? 'btn-primary' : 'btn-light'}`}
              onClick={() => handleQuickSelect(30, '30days')}
            >
              過去1ヶ月間
            </button>
            <button
              className={`btn btn-sm whitespace-nowrap ${selectedPeriod === '180days' ? 'btn-primary' : 'btn-light'}`}
              onClick={() => handleQuickSelect(180, '180days')}
            >
              過去半年
            </button>
            <button
              className={`btn btn-sm whitespace-nowrap ${selectedPeriod === '365days' ? 'btn-primary' : 'btn-light'}`}
              onClick={() => handleQuickSelect(365, '365days')}
            >
              過去1年
            </button>
            <button
              className={`btn btn-sm whitespace-nowrap ${selectedPeriod === 'custom' ? 'btn-primary' : 'btn-light'}`}
              onClick={handleCustomButtonClick}
            >
              カスタム
            </button>
        </div>
      </div>

      {selectedPeriod === 'custom' && (
        <>
          <div className="flex flex-wrap -mx-3">
            <div className="px-3 flex-1 min-w-0">
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-1">開始日時</label>
                <input
                  ref={startInputRef}
                  className="input"
                  type="datetime-local"
                  value={customStart}
                  min={inputLimits.min}
                  max={inputLimits.max}
                  onChange={(e) => setCustomStart(e.target.value)}
                  onKeyDown={handleKeyDown}
                  onFocus={() => handleInputFocus('start')}
                  onBlur={handleInputBlur}
                  title={inputLimits.min && inputLimits.max ? `利用可能な期間: ${inputLimits.min} ～ ${inputLimits.max}` : undefined}
                  style={{
                    transition: 'all 0.3s ease-in-out',
                    transform: focusedField === 'start' ? 'scale(1.02)' : 'scale(1)',
                    boxShadow: focusedField === 'start' ? '0 4px 12px rgba(0,209,178,0.3)' : 'none'
                  }}
                />
              </div>
            </div>
            <div className="px-3 flex-1 min-w-0">
              <div className="mb-4">
                <label className="block text-sm font-medium text-gray-700 mb-1">終了日時</label>
                <input
                  className="input"
                  type="datetime-local"
                  value={customEnd}
                  min={inputLimits.min}
                  max={inputLimits.max}
                  onChange={(e) => setCustomEnd(e.target.value)}
                  onKeyDown={handleKeyDown}
                  onFocus={() => handleInputFocus('end')}
                  onBlur={handleInputBlur}
                  title={inputLimits.min && inputLimits.max ? `利用可能な期間: ${inputLimits.min} ～ ${inputLimits.max}` : undefined}
                  style={{
                    transition: 'all 0.3s ease-in-out',
                    transform: focusedField === 'end' ? 'scale(1.02)' : 'scale(1)',
                    boxShadow: focusedField === 'end' ? '0 4px 12px rgba(0,209,178,0.3)' : 'none'
                  }}
                />
              </div>
            </div>
          </div>

          <div className="mb-4">
            <button
              className={`btn w-full ${hasChanges ? 'btn-primary' : 'btn-light'}`}
              onClick={handleCustomDateChange}
              disabled={!hasChanges}
              style={{
                transition: 'all 0.8s ease-in-out',
                transform: hasChanges ? 'scale(1)' : 'scale(0.98)',
                opacity: hasChanges ? 1 : 0.7
              }}
            >
              {hasChanges ? (
                <ArrowPathIcon className="w-5 h-5 mr-2 transition-transform duration-500" />
              ) : (
                <CheckIcon className="w-5 h-5 mr-2 transition-transform duration-500" />
              )}
              <span className="transition-all duration-500">
                {hasChanges ? '期間を確定して更新' : '変更なし'}
              </span>
            </button>
          </div>
        </>
      )}
      </div>

      <div className="mt-12 mb-5" id="altitude-selector">
        <div className={styles.sectionHeader}>
          <h2 className="text-2xl font-semibold whitespace-nowrap">
            <MountainIcon className="w-6 h-6 inline-block mr-2" />
            高度選択
            <LinkIcon
              className={`w-4 h-4 inline-block ${styles.permalinkIcon}`}
              onClick={() => copyPermalink('altitude-selector')}
              title="パーマリンクをコピー"
            />
          </h2>
        </div>

        <div className="mb-4">
          <label className="flex items-center cursor-pointer">
            <input
              type="checkbox"
              checked={limitAltitude}
              onChange={(e) => onAltitudeChange(e.target.checked)}
              className="mr-2"
            />
            <span>高度2,000m以下のみ表示</span>
          </label>
        </div>
      </div>

      <div ref={notificationRef} className={notificationClassName}></div>
    </>
  )
}

export default DateSelector
