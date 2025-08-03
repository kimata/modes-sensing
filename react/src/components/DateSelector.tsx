import { useState, useEffect, useRef } from 'react'
import styles from './GraphDisplay.module.css'

interface DateSelectorProps {
  startDate: Date
  endDate: Date
  onDateChange: (start: Date, end: Date) => void
}

const DateSelector: React.FC<DateSelectorProps> = ({ startDate, endDate, onDateChange }) => {
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
  const [selectedPeriod, setSelectedPeriod] = useState<'1day' | '7days' | '30days' | 'custom'>('7days')
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
      } else {
        setSelectedPeriod('custom')
      }
    } else {
      setSelectedPeriod('custom')
    }
  }, [startDate, endDate])

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

  const handleQuickSelect = (days: number, period: '1day' | '7days' | '30days') => {
    const end = new Date()
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定
    const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000)
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    onDateChange(start, end)
    setCustomStart(formatDateForInput(start))
    setCustomEnd(formatDateForInput(end))
    setSelectedPeriod(period)
  }

  const handleCustomDateChange = () => {
    const start = new Date(customStart)
    const end = new Date(customEnd)
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定
    if (start <= end) {
      onDateChange(start, end)
      setHasChanges(false)
      setSelectedPeriod('custom')
    } else {
      // 開始日時が終了日時より後の場合はエラー表示
      alert('開始日時は終了日時より前に設定してください')
    }
  }

  const handleCustomButtonClick = () => {
    setSelectedPeriod('custom')
    // カスタムボタンクリック時は入力フィールドにフォーカス
    const startInput = document.querySelector('input[type="datetime-local"]') as HTMLInputElement
    if (startInput) {
      startInput.focus()
    }
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

  return (
    <>
      <div className="box" id="date-selector">
        <div className={styles.sectionHeader}>
          <h2 className="title is-4">
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
        <div className="field is-grouped">
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '1day' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(1, '1day')}
            >
              過去24時間
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '7days' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(7, '7days')}
            >
              過去7日間
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === '30days' ? 'is-primary' : 'is-light'}`}
              onClick={() => handleQuickSelect(30, '30days')}
            >
              過去1ヶ月間
            </button>
          </div>
          <div className="control">
            <button
              className={`button is-small ${selectedPeriod === 'custom' ? 'is-primary' : 'is-light'}`}
              onClick={handleCustomButtonClick}
            >
              カスタム
            </button>
          </div>
        </div>
      </div>

      <div className="columns">
        <div className="column">
          <div className="field">
            <label className="label">開始日時</label>
            <div className="control">
              <input
                className={`input ${focusedField === 'start' ? 'is-focused' : ''}`}
                type="datetime-local"
                value={customStart}
                onChange={(e) => setCustomStart(e.target.value)}
                onKeyPress={handleKeyPress}
                onFocus={() => handleInputFocus('start')}
                onBlur={handleInputBlur}
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
                onChange={(e) => setCustomEnd(e.target.value)}
                onKeyPress={handleKeyPress}
                onFocus={() => handleInputFocus('end')}
                onBlur={handleInputBlur}
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
      </div>
      <div ref={notificationRef} className={styles.copyNotification}></div>
    </>
  )
}

export default DateSelector
