import { useMemo } from 'react'

function statusClass(code) {
  const n = Number(code)
  if (n >= 500) return 's5'
  if (n >= 400) return 's4'
  if (n >= 300) return 's3'
  return 's2'
}

function fmtBytes(n) {
  if (!n) return '0B'
  if (n > 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n > 1000) return `${(n / 1000).toFixed(0)}K`
  return `${n}B`
}

// Generate pseudo-random particle positions deterministically
function makeParticles(count) {
  const particles = []
  for (let i = 0; i < count; i++) {
    particles.push({
      left: `${(i * 37 + 13) % 100}%`,
      delay: `${((i * 0.7) % 3).toFixed(1)}s`,
      size: 1 + (i % 3),
      opacity: 0.15 + (i % 4) * 0.08,
    })
  }
  return particles
}

export default function DataStream({ events, live }) {
  const particles = useMemo(() => makeParticles(18), [])

  const stats = useMemo(() => {
    if (!events || events.length === 0) return null
    const total = events.length
    const errors = events.filter((e) => Number(e.status) >= 400).length
    const bytes = events.reduce((s, e) => s + (e.bytes || 0), 0)
    const hosts = new Set(events.map((e) => e.host)).size
    return { total, errors, bytes, hosts }
  }, [events])

  return (
    <section className="stream-panel">
      {/* Flowing particles */}
      {live && (
        <div className="stream-particles">
          {particles.map((p, i) => (
            <div
              key={i}
              className="particle"
              style={{
                left: p.left,
                width: p.size,
                height: p.size,
                animationDelay: p.delay,
                opacity: p.opacity,
              }}
            />
          ))}
        </div>
      )}

      <div className="stream-head">
        <div className="stream-title-row">
          <h3>Live request stream</h3>
          {events && events.length > 0 && (
            <span className="stream-count-badge">{events.length} events</span>
          )}
        </div>
        <span className={`stream-caption ${live ? 'live' : ''}`}>
          {live ? '● ClarkNet 1995 traces flowing through pipeline' : 'Waiting for replay...'}
        </span>
      </div>

      <div className="stream-track" style={{ position: 'relative' }}>
        {(!events || events.length === 0) && (
          <div className="empty-inline" style={{ margin: '0.5rem 0' }}>
            <span style={{ opacity: 0.6 }}>📡</span>{' '}
            Log lines will flow here as each window is processed
          </div>
        )}
        {(events || []).map((ev, i) => (
          <div
            key={`${ev.window_id}-${ev.seq}-${i}`}
            className={`stream-row ${statusClass(ev.status)}`}
            style={{ animationDelay: `${Math.min(i, 15) * 30}ms` }}
          >
            <span className="mono dim">W{ev.window_id}</span>
            <span className={`status-chip ${statusClass(ev.status)}`}>{ev.status}</span>
            <span className="mono method">{ev.method || 'GET'}</span>
            <span className="path" title={ev.path}>{ev.path}</span>
            <span className="host mono">{ev.host}</span>
            <span className="bytes mono dim">{fmtBytes(ev.bytes)}</span>
          </div>
        ))}
      </div>

      {stats && (
        <div className="stream-stats-bar">
          <div className="stream-stat">
            Events <span className="stream-stat-val">{stats.total}</span>
          </div>
          <div className="stream-stat">
            Errors <span className="stream-stat-val" style={{ color: stats.errors > 0 ? 'var(--red)' : undefined }}>{stats.errors}</span>
          </div>
          <div className="stream-stat">
            Volume <span className="stream-stat-val">{fmtBytes(stats.bytes)}</span>
          </div>
          <div className="stream-stat">
            Hosts <span className="stream-stat-val">{stats.hosts}</span>
          </div>
        </div>
      )}
    </section>
  )
}
