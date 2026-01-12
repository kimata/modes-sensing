import { useState, useEffect } from 'react'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import localizedFormat from 'dayjs/plugin/localizedFormat'
import 'dayjs/locale/ja'
import { SignalIcon, SignalSlashIcon } from '@heroicons/react/24/outline'
import { useApi } from '../hooks/useApi'
import type { LastReceived } from '../types'

dayjs.extend(relativeTime)
dayjs.extend(localizedFormat)
dayjs.locale('ja')

// 受信方式の表示設定
const RECEIVER_CONFIG = {
  mode_s: {
    label: 'Mode S',
    description: 'ADS-B (1090MHz)',
  },
  vdl2: {
    label: 'VDL2',
    description: 'VHF Data Link Mode 2',
  },
} as const

// 30分以上更新がない場合は警告表示
const STALE_THRESHOLD_MINUTES = 30

function ReceiverStatus() {
  const [now, setNow] = useState(dayjs())
  const { data: lastReceived, error } = useApi<LastReceived>(
    '/modes-sensing/api/last-received',
    { interval: 60000 } // 1分間隔でポーリング
  )

  // 現在時刻を1分ごとに更新（相対時間表示のため）
  useEffect(() => {
    const interval = setInterval(() => {
      setNow(dayjs())
    }, 60000)
    return () => clearInterval(interval)
  }, [])

  const formatTime = (isoString: string | null) => {
    if (!isoString) {
      return { relative: '受信なし', absolute: '', isStale: true, isNever: true }
    }

    const time = dayjs(isoString)
    const diffMinutes = now.diff(time, 'minute')
    const isStale = diffMinutes >= STALE_THRESHOLD_MINUTES

    return {
      relative: time.fromNow(),
      absolute: time.format('YY年M月D日 H:mm'),
      isStale,
      isNever: false,
    }
  }

  if (error) {
    return null // エラー時は何も表示しない
  }

  return (
    <div className="mb-6 p-4 bg-gray-50 rounded-lg">
      <h3 className="text-lg font-medium mb-3 flex items-center">
        <SignalIcon className="w-5 h-5 mr-2" />
        受信状況
      </h3>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {(['mode_s', 'vdl2'] as const).map((key) => {
          const config = RECEIVER_CONFIG[key]
          const time = formatTime(lastReceived?.[key] ?? null)

          return (
            <div
              key={key}
              className={`p-3 rounded border ${
                time.isNever
                  ? 'border-gray-300 bg-gray-100'
                  : time.isStale
                  ? 'border-yellow-400 bg-yellow-50'
                  : 'border-green-400 bg-green-50'
              }`}
            >
              <div className="flex items-center justify-between mb-1">
                <span className="font-medium">{config.label}</span>
                {time.isNever ? (
                  <SignalSlashIcon className="w-5 h-5 text-gray-400" />
                ) : time.isStale ? (
                  <SignalIcon className="w-5 h-5 text-yellow-500" />
                ) : (
                  <SignalIcon className="w-5 h-5 text-green-500" />
                )}
              </div>
              <div className="text-sm text-gray-600">{config.description}</div>
              <div className="mt-2">
                <span
                  className={`text-lg font-semibold ${
                    time.isNever
                      ? 'text-gray-500'
                      : time.isStale
                      ? 'text-yellow-700'
                      : 'text-green-700'
                  }`}
                >
                  {time.relative}
                </span>
                {!time.isNever && (
                  <span className="text-sm text-gray-500 ml-2">
                    ({time.absolute})
                  </span>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default ReceiverStatus
