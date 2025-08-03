import { useState, useEffect } from 'react'

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

  const handleQuickSelect = (days: number) => {
    const end = new Date()
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定
    const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000)
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    onDateChange(start, end)
    setCustomStart(formatDateForInput(start))
    setCustomEnd(formatDateForInput(end))
  }

  const handleCustomDateChange = () => {
    const start = new Date(customStart)
    const end = new Date(customEnd)
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定
    if (start <= end) {
      onDateChange(start, end)
      setHasChanges(false)
    } else {
      // 開始日時が終了日時より後の場合はエラー表示
      alert('開始日時は終了日時より前に設定してください')
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
    <div className="box">
      <h2 className="title is-4">
        <span className="icon" style={{ marginRight: '0.5em' }}>
          <i className="fas fa-calendar-alt"></i>
        </span>
        期間選択
      </h2>

      <div className="field">
        <label className="label">クイック選択</label>
        <div className="field is-grouped">
          <div className="control">
            <button
              className="button is-info is-small"
              onClick={() => handleQuickSelect(1)}
            >
              1日
            </button>
          </div>
          <div className="control">
            <button
              className="button is-info is-small"
              onClick={() => handleQuickSelect(7)}
            >
              7日
            </button>
          </div>
          <div className="control">
            <button
              className="button is-info is-small"
              onClick={() => handleQuickSelect(30)}
            >
              1ヶ月
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
  )
}

export default DateSelector
