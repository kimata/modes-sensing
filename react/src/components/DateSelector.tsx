import { useState, useEffect, useRef } from 'react'
import styles from './GraphDisplay.module.css'

interface DataRange {
  earliest: string | null
  latest: string | null
  count?: number
}

interface DateSelectorProps {
  startDate: Date
  endDate: Date
  onDateChange: (start: Date, end: Date) => void
  dataRange: DataRange | null
  limitAltitude: boolean
  onAltitudeChange: (limited: boolean) => void
}

const DateSelector: React.FC<DateSelectorProps> = ({ startDate, endDate, onDateChange, dataRange, limitAltitude, onAltitudeChange }) => {
  const formatDateForInput = (date: Date): string => {
    const year = date.getFullYear()
    const month = String(date.getMonth() + 1).padStart(2, '0')
    const day = String(date.getDate()).padStart(2, '0')
    const hours = String(date.getHours()).padStart(2, '0')
    const minutes = String(date.getMinutes()).padStart(2, '0')
    return `${year}-${month}-${day}T${hours}:${minutes}`
  }

  const [customStart, setCustomStart] = useState(formatDateForInput(startDate))
  const [customEnd, setCustomEnd] = useState(formatDateForInput(endDate))
  const [hasChanges, setHasChanges] = useState(false)
  const [focusedField, setFocusedField] = useState<'start' | 'end' | null>(null)
  const [selectedPeriod, setSelectedPeriod] = useState<'1day' | '7days' | '30days' | '180days' | '365days' | 'custom'>('7days')
  const [userSelectedPeriod, setUserSelectedPeriod] = useState<'1day' | '7days' | '30days' | '180days' | '365days' | 'custom' | null>('7days') // デフォルトで7日間を明示的に設定
  const [isQuickSelectActive, setIsQuickSelectActive] = useState(false) // 期間ボタン押下後の自動判定を抑制
  const notificationRef = useRef<HTMLDivElement>(null)

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

  // 現在の期間から選択されているボタンを判定
  useEffect(() => {
    // 期間ボタン押下直後は自動判定をスキップ
    if (isQuickSelectActive) {
      setIsQuickSelectActive(false)
      return
    }

    // ユーザーが明示的に期間ボタンを選択した場合、その選択を維持
    if (userSelectedPeriod && userSelectedPeriod !== 'custom') {
      setSelectedPeriod(userSelectedPeriod)
      return
    }

    const now = new Date()
    const diffMs = endDate.getTime() - startDate.getTime()
    const diffDays = Math.round(diffMs / (24 * 60 * 60 * 1000))

    // 終了日時が現在時刻に近い（1時間以内）かつ、期間が特定の日数に近い場合
    const isNearNow = Math.abs(now.getTime() - endDate.getTime()) < 60 * 60 * 1000

    if (isNearNow) {
      if (Math.abs(diffDays - 1) < 0.1) {
        setSelectedPeriod('1day')
      } else if (Math.abs(diffDays - 7) < 0.1) {
        setSelectedPeriod('7days')
      } else if (Math.abs(diffDays - 30) < 0.5) {
        setSelectedPeriod('30days')
      } else if (Math.abs(diffDays - 180) < 1) {
        setSelectedPeriod('180days')
      } else if (Math.abs(diffDays - 365) < 2) {
        setSelectedPeriod('365days')
      } else {
        setSelectedPeriod('custom')
      }
    } else {
      setSelectedPeriod('custom')
    }
  }, [startDate, endDate, isQuickSelectActive, userSelectedPeriod])

  // 初期表示時のdataRangeによる日付調整はApp.tsxで実行済みのため削除

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

  const handleQuickSelect = (days: number, period: '1day' | '7days' | '30days' | '180days' | '365days') => {
    let end = new Date()
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定
    let start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000)
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定


    // データ範囲による調整
    if (dataRange && dataRange.earliest && dataRange.latest) {
      const dataEarliest = new Date(dataRange.earliest)
      const dataLatest = new Date(dataRange.latest)

      // 終了日時が利用可能なデータの最新日時を超えている場合、データの最新日時に調整
      if (end > dataLatest) {
        end = new Date(dataLatest)
        end.setSeconds(0, 0)
      }

      // 開始日時が利用可能なデータの最古日時を下回っている場合、データの最古日時に調整
      if (start < dataEarliest) {
        start = new Date(dataEarliest)
        start.setSeconds(0, 0)
      }

      // 期間が調整された場合、終了日から逆算して適切な期間を設定
      const requestedPeriodMs = days * 24 * 60 * 60 * 1000
      const adjustedStart = new Date(end.getTime() - requestedPeriodMs)
      if (adjustedStart >= dataEarliest) {
        start = adjustedStart
        start.setSeconds(0, 0)
      }
    }

    // 期間選択状態を先に設定（useEffectによる自動判定を防ぐ）
    setSelectedPeriod(period)
    setUserSelectedPeriod(period) // ユーザーが明示的に選択した期間として記録
    setIsQuickSelectActive(true) // 自動判定を抑制するフラグを設定

    onDateChange(start, end)
    setCustomStart(formatDateForInput(start))
    setCustomEnd(formatDateForInput(end))
  }

  const handleCustomDateChange = () => {
    let start = new Date(customStart)
    let end = new Date(customEnd)
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定

    // 基本的な日付順序チェック
    if (start > end) {
      alert('開始日時は終了日時より前に設定してください')
      return
    }

    // データ範囲による自動調整
    let adjusted = false
    if (dataRange && dataRange.earliest && dataRange.latest) {
      const dataEarliest = new Date(dataRange.earliest)
      const dataLatest = new Date(dataRange.latest)

      if (start < dataEarliest) {
        start = new Date(dataEarliest)
        start.setSeconds(0, 0)
        adjusted = true
      }

      if (end > dataLatest) {
        end = new Date(dataLatest)
        end.setSeconds(0, 0)
        adjusted = true
      }

      // 調整後の順序チェック
      if (start > end) {
        const earliestStr = dataEarliest.toLocaleDateString('ja-JP') + ' ' + dataEarliest.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' })
        const latestStr = dataLatest.toLocaleDateString('ja-JP') + ' ' + dataLatest.toLocaleTimeString('ja-JP', { hour: '2-digit', minute: '2-digit' })
        alert(`利用可能な期間（${earliestStr} ～ ${latestStr}）では、指定された期間を設定できません`)
        return
      }

      if (adjusted) {
        // Date range was adjusted to fit available data
      }
    }

    onDateChange(start, end)
    setHasChanges(false)
    setSelectedPeriod('custom')
    setUserSelectedPeriod('custom') // カスタム期間として記録

    // 調整された場合は入力フィールドも更新
    if (adjusted) {
      setCustomStart(formatDateForInput(start))
      setCustomEnd(formatDateForInput(end))
    }
  }

  const handleCustomButtonClick = () => {
    // 直前に選択されていた期間ボタンに基づいて日時を設定
    if (selectedPeriod !== 'custom') {
      const periodDays = {
        '1day': 1,
        '7days': 7,
        '30days': 30,
        '180days': 180,
        '365days': 365
      }

      const days = periodDays[selectedPeriod]
      if (days) {
        let end = new Date()
        end.setSeconds(0, 0)
        let start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000)
        start.setSeconds(0, 0)

        // データ範囲による調整
        if (dataRange && dataRange.earliest && dataRange.latest) {
          const dataEarliest = new Date(dataRange.earliest)
          const dataLatest = new Date(dataRange.latest)

          if (end > dataLatest) {
            end = new Date(dataLatest)
            end.setSeconds(0, 0)
          }

          if (start < dataEarliest) {
            start = new Date(dataEarliest)
            start.setSeconds(0, 0)
          }

          const requestedPeriodMs = days * 24 * 60 * 60 * 1000
          const adjustedStart = new Date(end.getTime() - requestedPeriodMs)
          if (adjustedStart >= dataEarliest) {
            start = adjustedStart
            start.setSeconds(0, 0)
          }
        }

        setCustomStart(formatDateForInput(start))
        setCustomEnd(formatDateForInput(end))
      }
    }

    setSelectedPeriod('custom')
    setUserSelectedPeriod('custom') // ユーザーが明示的にカスタムを選択

    // カスタムボタンクリック時は入力フィールドにフォーカス
    setTimeout(() => {
      const startInput = document.querySelector('input[type="datetime-local"]') as HTMLInputElement
      if (startInput) {
        startInput.focus()
      }
    }, 100)
  }

  const handleKeyPress = (e: React.KeyboardEvent) => {
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
      <div className="box" id="date-selector">
        <div className={styles.sectionHeader}>
          <h2 className="title is-4" style={{ whiteSpace: 'nowrap' }}>  {/* 見出しの改行防止 */}
            <span className="icon" style={{ marginRight: '0.5em' }}>
              <i className="fas fa-calendar-alt"></i>
            </span>
            期間選択
            <i
              className={`fas fa-link ${styles.permalinkIcon}`}
              onClick={() => copyPermalink('date-selector')}
              title="パーマリンクをコピー"
            />
          </h2>
        </div>

      <div className="field">
        <label className="label">クイック選択</label>
        <div className="field is-grouped is-grouped-multiline">
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '1day' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(1, '1day')}
              style={{ whiteSpace: 'nowrap' }}  /* ボタンテキストの改行防止 */
            >
              過去24時間
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '7days' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(7, '7days')}
              style={{ whiteSpace: 'nowrap' }}  /* ボタンテキストの改行防止 */
            >
              過去7日間
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '30days' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(30, '30days')}
              style={{ whiteSpace: 'nowrap' }}  /* ボタンテキストの改行防止 */
            >
              過去1ヶ月間
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '180days' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(180, '180days')}
              style={{ whiteSpace: 'nowrap' }}  /* ボタンテキストの改行防止 */
            >
              過去半年
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '365days' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(365, '365days')}
              style={{ whiteSpace: 'nowrap' }}  /* ボタンテキストの改行防止 */
            >
              過去1年
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === 'custom' ? 'is-primary' : 'is-light'}`}
              onClick={handleCustomButtonClick}
              style={{ whiteSpace: 'nowrap' }}  /* ボタンテキストの改行防止 */
            >
              カスタム
            </button>
          </div>
        </div>
      </div>

      {selectedPeriod === 'custom' && (
        <>
          <div className="columns">
            <div className="column">
              <div className="field">
                <label className="label">開始日時</label>
                <div className="control">
                  <input
                    className={`input ${focusedField === 'start' ? 'is-focused' : ''}`}
                    type="datetime-local"
                    value={customStart}
                    min={inputLimits.min}
                    max={inputLimits.max}
                    onChange={(e) => setCustomStart(e.target.value)}
                    onKeyPress={handleKeyPress}
                    onFocus={() => handleInputFocus('start')}
                    onBlur={handleInputBlur}
                    title={inputLimits.min && inputLimits.max ? `利用可能な期間: ${inputLimits.min} ～ ${inputLimits.max}` : undefined}
                    style={{
                      transition: 'all 0.3s ease-in-out',
                      transform: focusedField === 'start' ? 'scale(1.02)' : 'scale(1)',
                      boxShadow: focusedField === 'start' ? '0 4px 12px rgba(0,123,255,0.3)' : 'none'
                    }}
                  />
                </div>
              </div>
            </div>
            <div className="column">
              <div className="field">
                <label className="label">終了日時</label>
                <div className="control">
                  <input
                    className={`input ${focusedField === 'end' ? 'is-focused' : ''}`}
                    type="datetime-local"
                    value={customEnd}
                    min={inputLimits.min}
                    max={inputLimits.max}
                    onChange={(e) => setCustomEnd(e.target.value)}
                    onKeyPress={handleKeyPress}
                    onFocus={() => handleInputFocus('end')}
                    onBlur={handleInputBlur}
                    title={inputLimits.min && inputLimits.max ? `利用可能な期間: ${inputLimits.min} ～ ${inputLimits.max}` : undefined}
                    style={{
                      transition: 'all 0.3s ease-in-out',
                      transform: focusedField === 'end' ? 'scale(1.02)' : 'scale(1)',
                      boxShadow: focusedField === 'end' ? '0 4px 12px rgba(0,123,255,0.3)' : 'none'
                    }}
                  />
                </div>
              </div>
            </div>
          </div>

          <div className="field">
            <div className="control">
              <button
                className={`button is-fullwidth ${hasChanges ? 'is-primary' : 'is-light'}`}
                onClick={handleCustomDateChange}
                disabled={!hasChanges}
                style={{
                  transition: 'all 0.8s ease-in-out',
                  backgroundColor: hasChanges ? undefined : '#f5f5f5',
                  borderColor: hasChanges ? undefined : '#dbdbdb',
                  color: hasChanges ? undefined : '#7a7a7a',
                  transform: hasChanges ? 'scale(1)' : 'scale(0.98)',
                  opacity: hasChanges ? 1 : 0.7
                }}
              >
                <span className="icon" style={{ transition: 'transform 0.3s ease-in-out' }}>
                  <i
                    className={`fas ${hasChanges ? 'fa-sync-alt' : 'fa-check'}`}
                    style={{
                      transition: 'all 0.5s ease-in-out',
                      transform: hasChanges ? 'rotate(0deg)' : 'rotate(360deg)'
                    }}
                  ></i>
                </span>
                <span style={{ transition: 'all 0.5s ease-in-out' }}>
                  {hasChanges ? '期間を確定して更新' : '変更なし'}
                </span>
              </button>
            </div>
          </div>
        </>
      )}
      </div>

      <div className="box" id="altitude-selector">
        <div className={styles.sectionHeader}>
          <h2 className="title is-4" style={{ whiteSpace: 'nowrap' }}>
            <span className="icon" style={{ marginRight: '0.5em' }}>
              <i className="fas fa-mountain"></i>
            </span>
            高度選択
            <i
              className={`fas fa-link ${styles.permalinkIcon}`}
              onClick={() => copyPermalink('altitude-selector')}
              title="パーマリンクをコピー"
            />
          </h2>
        </div>

        <div className="field">
          <div className="control">
            <label className="checkbox" style={{ display: 'flex', alignItems: 'center', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={limitAltitude}
                onChange={(e) => onAltitudeChange(e.target.checked)}
                style={{ marginRight: '0.5rem' }}
              />
              <span>高度2,000m以下のみ表示</span>
            </label>
          </div>
        </div>
      </div>

      <div ref={notificationRef} className={styles.copyNotification}></div>
    </>
  )
}

export default DateSelector
