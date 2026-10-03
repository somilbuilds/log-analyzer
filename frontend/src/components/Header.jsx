export default function Header({ status, busy, onStart, onStop }) {
  const isLive = status.running

  let timeStr = '—'
  if (status.last_update) {
    try { timeStr = status.last_update.split('T')[1].substring(0, 8) } catch { timeStr = '—' }
  }

  const stages = [
    { label: 'ClarkNet', icon: '📦', on: true },
    { label: 'Replayer', icon: '🔄', on: status.demo?.replayer || isLive },
    { label: 'Spark Stream', icon: '⚡', on: status.demo?.stream || isLive },
    { label: 'MongoDB', icon: '🗄️', on: status.mongo },
    { label: 'Dashboard', icon: '📊', on: true },
  ]

  return (
    <header className="app-header">
      <div className="header-top">
        <div className="header-brand">
          <h1>⚡ ClarkNet Log Analyzer</h1>
          <p className="subtitle">
            Real-time replay of 1.6M ClarkNet-HTTP 1995 traces · Bloom · DGIM · Flajolet–Martin
          </p>
        </div>
        <div className="header-right">
          <span className="meta-chip">
            {timeStr !== '—' ? `⏱ ${timeStr}` : ''}
          </span>
          <span className={`live-pill ${isLive ? 'on' : ''}`}>
            <span className="live-dot" />
            {isLive ? 'STREAMING' : 'IDLE'}
          </span>
        </div>
      </div>

      <div className="header-pipeline">
        <div className="pipeline">
          {stages.map((step, i) => (
            <span key={step.label} className="pipeline-item">
              <span className={`stage ${step.on ? 'on' : ''}`}>
                {step.icon} {step.label}
              </span>
              {i < stages.length - 1 && (
                <span className={`pipe-flow ${isLive ? 'moving' : ''}`} />
              )}
            </span>
          ))}
          <span className="window-count">{status.window_count || 0} windows processed</span>
        </div>

        <div className="header-actions">
          {(!status.demo?.replayer && !status.demo?.stream) ? (
            <div style={{ display: 'flex', gap: '0.6rem' }}>
              <button className="btn" onClick={() => onStart(true)} disabled={busy} style={{ background: 'var(--surface-alt)' }}>
                ▶ Continue stream
              </button>
              <button className="btn primary" onClick={() => onStart(false)} disabled={busy}>
                🔄 Restart fresh
              </button>
            </div>
          ) : (
            <button className="btn danger" onClick={onStop} disabled={busy}>
              ⏹ Stop
            </button>
          )}
        </div>
      </div>
    </header>
  )
}
