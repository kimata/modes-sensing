import { useState, useEffect } from 'react'
import dayjs from 'dayjs'
import relativeTime from 'dayjs/plugin/relativeTime'
import localizedFormat from 'dayjs/plugin/localizedFormat'
import 'dayjs/locale/ja'
import { version as reactVersion } from 'react'
import { useApi } from '../hooks/useApi'
import type { SysInfo } from '../types'

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
    <div className="is-pulled-right has-text-right p-2 mt-4" data-testid="footer">
      <div className="is-size-6">
        <p className="has-text-grey mb-0 is-size-7">
          更新日時: {updateTime} (commit: {commitHash})
        </p>
        <p className="has-text-grey mb-0 is-size-7">
          イメージビルド: {getImageBuildDate()}
        </p>
        <p className="has-text-grey mb-0 is-size-7">
          React ビルド: {buildDate.format('YYYY年MM月DD日 HH:mm:ss')} [{buildDate.fromNow()}]
        </p>
        <p className="has-text-grey mb-0 is-size-7">
          React バージョン: {reactVersion}
        </p>
        <p className="is-size-2">
          <a
            href="https://github.com/kimata/modes-sensing"
            className="has-text-grey-light"
          >
            <i className="fab fa-github"></i>
          </a>
        </p>
      </div>
    </div>
  )
}

export default Footer
