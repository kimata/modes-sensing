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
    <div className="modal is-active">
      <div className="modal-background" onClick={onClose}></div>
      <div
        className="modal-content"
        style={{
          width: '95vw',      // 画面幅の95%
          maxWidth: 'none',   // Bulmaのデフォルト制限を解除
          margin: '0 auto'    // 中央配置
        }}
      >
        <p className="image">
          <img
            src={imageUrl}
            alt="拡大表示"
            style={{
              width: '100%',      // コンテナ幅に合わせる
              height: 'auto',     // アスペクト比を保持
              maxHeight: '90vh',  // 画面高さの90%まで
              objectFit: 'contain'
            }}
          />
        </p>
      </div>
      <button
        className="modal-close is-large"
        aria-label="close"
        onClick={onClose}
      ></button>
    </div>
  )
}

export default Modal
