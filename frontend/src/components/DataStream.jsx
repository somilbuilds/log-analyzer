import { useMemo } from 'react'

function statusClass(code) {
  const n = Number(code)
  if (n >= 500) return 's5'
  if (n >= 400) return 's4'
  if (n >= 300) return 's3'
  return 's2'
}

function fmtBytes(n) {
  if (!n) return '0 B'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)} MB`
  if (n >= 1000) return `${(n / 1000).toFixed(1)} KB`
  return `${n} B`
}

function fmtTime(ev) {
  const raw = ev.timestamp || ev.time || ev.created_at || ev.ts
  if (!raw) return `W${ev.window_id ?? '—'}`
  try {
    const date = new Date(raw)
    if (!Number.isNaN(date.getTime())) {
      return date.toLocaleTimeString('en-IN', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit', fractionalSecondDigits: 3 })
    }
  } catch {
    // fall through to raw value
  }
  return String(raw).split(' ')[0]
}

export default function DataStream({ events, live }) {
  const rows = useMemo(() => [...(events || [])].slice(0, 80), [events])
  const stats = useMemo(() => {
    if (!rows.length) return null
    const errors = rows.filter((e) => Number(e.status) >= 400).length
    const serverErrors = rows.filter((e) => Number(e.status) >= 500).length
    const bytes = rows.reduce((sum, e) => sum + (Number(e.bytes) || 0), 0)
    const hosts = new Set(rows.map((e) => e.host).filter(Boolean)).size
    return { total: rows.length, errors, serverErrors, bytes, hosts }
  }, [rows])

  return (
    <section className="log-console">
      <div className="panel-heading log-heading">
        <div>
          <h2>Live Log Stream</h2>
          <p>Incoming ClarkNet records from the replay feed. The newest row is highlighted as each poll receives fresh data.</p>
        </div>
        <span className={`stream-state ${live ? 'running' : ''}`}>{live ? 'receiving records' : 'waiting for replay'}</span>
      </div>

      <div className="log-table" role="table" aria-label="Live ClarkNet log records">
        <div className="log-row log-head" role="row">
          <span>Time</span>
          <span>Status</span>
          <span>Method</span>
          <span>Endpoint / path</span>
          <span>Host</span>
          <span>Bytes</span>
        </div>

        {!rows.length && (
          <div className="empty-inline log-empty">Start or continue the replay to see real ClarkNet request records arrive here.</div>
        )}

        {rows.map((ev, index) => (
          <div
            key={`${ev.window_id}-${ev.seq}-${ev.path}-${index}`}
            className={`log-row ${statusClass(ev.status)} ${index === 0 ? 'newest' : ''}`}
            role="row"
          >
            <span className="mono dim">{fmtTime(ev)}</span>
            <span><span className={`status-chip ${statusClass(ev.status)}`}>{ev.status || '—'}</span></span>
            <span className="mono method">{ev.method || 'GET'}</span>
            <span className="log-path" title={ev.path || ''}>{ev.path || '/'}</span>
            <span className="mono log-host" title={ev.host || ''}>{ev.host || 'unknown'}</span>
            <span className="mono dim">{fmtBytes(Number(ev.bytes) || 0)}</span>
          </div>
        ))}
      </div>

      {stats && (
        <div className="log-summary">
          <span><strong>{stats.total}</strong> visible records</span>
          <span><strong>{stats.hosts}</strong> hosts</span>
          <span><strong>{fmtBytes(stats.bytes)}</strong> transferred</span>
          <span className={stats.errors ? 'warn-text' : ''}><strong>{stats.errors}</strong> 4xx/5xx</span>
          <span className={stats.serverErrors ? 'error-text' : ''}><strong>{stats.serverErrors}</strong> 5xx</span>
        </div>
      )}
    </section>
  )
}
