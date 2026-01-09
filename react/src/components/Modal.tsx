import { useEffect } from 'react'

interface ModalProps {
  imageUrl: string
  onClose: () => void
}

const Modal: React.FC<ModalProps> = ({ imageUrl, onClose }) => {
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose()
      }
    }

    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose}></div>
      <div
        className="relative z-10 mx-auto"
        style={{
          width: '95vw',
          maxWidth: 'none',
          margin: '0 auto'
        }}
      >
        <div className="block">
          <img
            src={imageUrl}
            alt="拡大表示"
            className="w-full h-auto max-h-[90vh] object-contain"
          />
        </div>
      </div>
      <button
        className="absolute top-4 right-4 w-10 h-10 rounded-full bg-black/30 text-white flex items-center justify-center hover:bg-black/50 transition-colors cursor-pointer"
        aria-label="close"
        onClick={onClose}
      >
        <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
        </svg>
      </button>
    </div>
  )
}

export default Modal
