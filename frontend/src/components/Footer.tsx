import { useState, useEffect } from 'react'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import localizedFormat from 'dayjs/plugin/localizedFormat'
import 'dayjs/locale/ja'
import { version as reactVersion } from 'react'
import { useApi } from '../hooks/useApi'
import type { SysInfo } from '../types'

// GitHub アイコン（Heroicons にないためカスタム SVG）
const GitHubIcon: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} fill="currentColor" viewBox="0 0 24 24">
    <path fillRule="evenodd" clipRule="evenodd" d="M12 2C6.477 2 2 6.477 2 12c0 4.42 2.865 8.17 6.839 9.49.5.092.682-.217.682-.482 0-.237-.008-.866-.013-1.7-2.782.604-3.369-1.34-3.369-1.34-.454-1.156-1.11-1.464-1.11-1.464-.908-.62.069-.608.069-.608 1.003.07 1.531 1.03 1.531 1.03.892 1.529 2.341 1.087 2.91.831.092-.646.35-1.086.636-1.336-2.22-.253-4.555-1.11-4.555-4.943 0-1.091.39-1.984 1.029-2.683-.103-.253-.446-1.27.098-2.647 0 0 .84-.269 2.75 1.025A9.578 9.578 0 0112 6.836c.85.004 1.705.114 2.504.336 1.909-1.294 2.747-1.025 2.747-1.025.546 1.377.203 2.394.1 2.647.64.699 1.028 1.592 1.028 2.683 0 3.842-2.339 4.687-4.566 4.935.359.309.678.919.678 1.852 0 1.336-.012 2.415-.012 2.743 0 .267.18.578.688.48C19.138 20.167 22 16.418 22 12c0-5.523-4.477-10-10-10z" />
  </svg>
)

dayjs.extend(relativeTime)
dayjs.extend(localizedFormat)
dayjs.locale('ja')

function Footer() {
  const [updateTime, setUpdateTime] = useState(dayjs().format('YYYY年MM月DD日 HH:mm:ss'))
  const buildDate = dayjs(import.meta.env.VITE_BUILD_DATE || new Date().toISOString())
  const commitHash = import.meta.env.VITE_GIT_COMMIT_HASH || 'unknown'
  const { data: sysInfo } = useApi<SysInfo>('/modes-sensing/api/sysinfo', { interval: 300000 }) // 5分間隔で更新

  useEffect(() => {
    // 定期的に更新時刻を更新
    const interval = setInterval(() => {
      setUpdateTime(dayjs().format('YYYY年MM月DD日 HH:mm:ss'))
    }, 60000) // 1分間隔

    return () => clearInterval(interval)
  }, [])

  const getImageBuildDate = () => {
    if (!sysInfo?.image_build_date) return 'Unknown'
    const buildDate = dayjs(sysInfo.image_build_date)
    return `${buildDate.format('YYYY年MM月DD日 HH:mm:ss')} [${buildDate.fromNow()}]`
  }

  return (
    <div className="ml-auto text-right p-2 mt-2" data-testid="footer">
      <div className="text-base">
        <p className="text-gray-500 mb-0 text-sm">
          更新日時: {updateTime} (commit: {commitHash})
        </p>
        <p className="text-gray-500 mb-0 text-sm">
          イメージビルド: {getImageBuildDate()}
        </p>
        <p className="text-gray-500 mb-0 text-sm">
          React ビルド: {buildDate.format('YYYY年MM月DD日 HH:mm:ss')} [{buildDate.fromNow()}]
        </p>
        <p className="text-gray-500 mb-0 text-sm">
          React バージョン: {reactVersion}
        </p>
        <p className="flex justify-end">
          <a
            href="https://github.com/kimata/modes-sensing"
            className="text-gray-400 hover:text-gray-500 transition-colors"
          >
            <GitHubIcon className="w-10 h-10" />
          </a>
        </p>
      </div>
    </div>
  )
}

export default Footer
