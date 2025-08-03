import { useState } from 'react'
import DateSelector from './components/DateSelector'
import GraphDisplay from './components/GraphDisplay'
import Modal from './components/Modal'
import Footer from './components/Footer'

function App() {
  const getInitialDate = () => {
    const end = new Date()
    end.setSeconds(0, 0) // 秒とミリ秒を0に設定
    const start = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) // 7 days ago
    start.setSeconds(0, 0) // 秒とミリ秒を0に設定
    return { start, end }
  }

  const [dateRange, setDateRange] = useState(getInitialDate())
  const [modalImage, setModalImage] = useState<string | null>(null)

  const handleDateChange = (start: Date, end: Date) => {
    setDateRange({ start, end })
  }

  const handleImageClick = (imageUrl: string) => {
    setModalImage(imageUrl)
  }

  const handleModalClose = () => {
    setModalImage(null)
  }

  return (
    <div className="container">
      <section className="section">
        <div className="container">
          <h1 className="title is-2 has-text-centered">
            <span className="icon is-large" style={{ marginRight: '0.5em' }}>
              <i className="fas fa-plane"></i>
            </span>
            航空機の気象データ
            <span style={{ marginLeft: '0.5em' }}></span>
          </h1>

          <DateSelector
            startDate={dateRange.start}
            endDate={dateRange.end}
            onDateChange={handleDateChange}
          />

          <GraphDisplay
            dateRange={dateRange}
            onImageClick={handleImageClick}
          />
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
