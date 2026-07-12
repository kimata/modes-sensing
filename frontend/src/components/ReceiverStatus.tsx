import { useState, useEffect } from 'react'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import localizedFormat from 'dayjs/plugin/localizedFormat'
import 'dayjs/locale/ja'
import { SignalIcon, SignalSlashIcon, ExclamationTriangleIcon } from '@heroicons/react/24/outline'
import { useApi } from '../hooks/useApi'
import type { ReceiverQualityResponse } from '../types'

dayjs.extend(relativeTime)
dayjs.extend(localizedFormat)
dayjs.locale('ja')

// 受信方式の表示設定
const RECEIVER_CONFIG = {
  mode_s: {
    label: 'Mode S',
    description: 'ADS-B (1090MHz)',
    staleThresholdMinutes: 30, // 30分以上更新がない場合は警告
  },
  vdl2: {
    label: 'VDL2',
    description: 'VHF Data Link Mode 2',
    staleThresholdMinutes: 360, // 6時間以上更新がない場合は警告
  },
} as const

function ReceiverStatus() {
  const [now, setNow] = useState(dayjs())
  const { data: quality, error } = useApi<ReceiverQualityResponse>(
    '/modes-sensing/api/receiver-quality',
    { interval: 60000 } // 1分間隔でポーリング
  )

  // 現在時刻を1分ごとに更新（相対時間表示のため）
  useEffect(() => {
    const interval = setInterval(() => {
      setNow(dayjs())
    }, 60000)
    return () => clearInterval(interval)
  }, [])

  const formatTime = (isoString: string | null, staleThresholdMinutes: number) => {
    if (!isoString) {
      return { relative: '受信なし', absolute: '', isStale: true, isNever: true }
    }

    const time = dayjs(isoString)
    const diffMinutes = now.diff(time, 'minute')
    const isStale = diffMinutes >= staleThresholdMinutes

    return {
      relative: time.fromNow(),
      absolute: time.format('YY年M月D日 H:mm'),
      isStale,
      isNever: false,
    }
  }

  // 初回ロード中（エラーもデータもない）は何も表示しない
  if (!quality && !error) {
    return null
  }

  return (
    <div className="mt-12 mb-0">
      <h2 className="text-2xl font-semibold whitespace-nowrap mb-4">
        <SignalIcon className="w-6 h-6 inline-block mr-2" />
        受信状況
      </h2>
      {error || !quality ? (
        <div className="p-3 rounded border border-red-300 bg-red-50 text-red-700 flex items-center">
          <ExclamationTriangleIcon className="w-5 h-5 mr-2" />
          受信状況を取得できませんでした
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {(['mode_s', 'vdl2'] as const).map((key) => {
            const config = RECEIVER_CONFIG[key]
            const method = quality[key]
            const time = formatTime(method.last_received, config.staleThresholdMinutes)

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
                <div className="mt-2 text-sm text-gray-600 flex flex-wrap gap-x-4">
                  <span className="whitespace-nowrap">
                    直近1時間: <span className="font-semibold">{method.last_hour.toLocaleString('ja-JP')}</span> 件
                  </span>
                  <span className="whitespace-nowrap">
                    24時間: <span className="font-semibold">{method.last_24h.toLocaleString('ja-JP')}</span> 件
                  </span>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

export default ReceiverStatus
