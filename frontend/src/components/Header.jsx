export default function Header({ status, busy, onStart, onStop }) {
  const isLive = status.running
  const timeStr = status.last_update ? status.last_update.split('T')[1]?.substring(0, 8) || '—' : '—'

  return (
    <header className="page-header streaming-header">
      <div>
        <p className="eyebrow">Spark Structured Streaming</p>
        <h1>Live Streaming Analytics</h1>
        <p className="subtitle">ClarkNet HTTP logs replayed as micro-batches with Bloom Filter, DGIM, and Flajolet-Martin analysis.</p>
      </div>
      <div className="stream-controls">
        <span className={`live-pill ${isLive ? 'on' : ''}`}>
          <span className="live-dot" />
          {isLive ? 'Streaming' : 'Idle'}
        </span>
        <span className="meta-chip">Window {status.window_count || 0}</span>
        <span className="meta-chip">Last update {timeStr}</span>
        {(!status.demo?.replayer && !status.demo?.stream) ? (
          <>
            <button className="btn" onClick={() => onStart(true)} disabled={busy}>Continue</button>
            <button className="btn primary" onClick={() => onStart(false)} disabled={busy}>Restart</button>
          </>
        ) : (
          <button className="btn danger" onClick={onStop} disabled={busy}>Stop stream</button>
        )}
      </div>
    </header>
  )
}
