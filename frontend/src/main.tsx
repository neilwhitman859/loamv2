import { StrictMode, Component } from 'react'
import type { ReactNode, ErrorInfo } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import './index.css'
import App from './App'

class ErrorBoundary extends Component<{ children: ReactNode }, { hasError: boolean }> {
  state = { hasError: false }
  static getDerivedStateFromError() { return { hasError: true } }
  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('ErrorBoundary caught:', error, info.componentStack)
  }
  render() {
    if (this.state.hasError) {
      return (
        <div style={{ maxWidth: '32rem', margin: '4rem auto', padding: '0 1rem', textAlign: 'center' }}>
          <h1 style={{ fontSize: '1.5rem', fontWeight: 600, color: '#1a1a1a', marginBottom: '0.5rem' }}>Something went wrong</h1>
          <p style={{ color: '#888', fontSize: '0.875rem', marginBottom: '1rem' }}>An unexpected error occurred.</p>
          <button
            onClick={() => { this.setState({ hasError: false }); window.location.href = '/' }}
            style={{ padding: '0.5rem 1.5rem', fontSize: '0.875rem', color: '#fff', background: '#7c2d12', border: 'none', borderRadius: '0.5rem', cursor: 'pointer' }}
          >
            Back to home
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ErrorBoundary>
      <BrowserRouter>
        <App />
      </BrowserRouter>
    </ErrorBoundary>
  </StrictMode>,
)
