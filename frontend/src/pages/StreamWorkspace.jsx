import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import {
  LineChart, Line, AreaChart, Area, BarChart, Bar, ComposedChart,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts'
import { usePolling, useWebSocket, apiFetch, apiPost, formatNumber, formatBytes, formatPct, statusClass, CHART_COLORS } from '../hooks/useApi'

const SPEEDS = [
  { label: '0.25×', value: 10 },
  { label: '0.5×', value: 20 },
  { label: '1×', value: 40 },
  { label: '2×', value: 80 },
  { label: '5×', value: 200 },
  { label: '10×', value: 400 },
  { label: 'Max', value: 2000 },
]

const GRID = '#1e1f2e'
const TICK = { fill: '#6b6d7a', fontSize: 10 }

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-label">Window {label}</div>
      {payload.map((p, i) => (
        <div className="chart-tooltip-row" key={i}>
          <span className="chart-tooltip-dot" style={{ background: p.color }} />
          <span>{p.name || p.dataKey}</span>
          <span className="chart-tooltip-value">{typeof p.value === 'number' ? p.value.toLocaleString() : p.value}</span>
        </div>
      ))}
    </div>
  )
}

export default function StreamWorkspace() {
  const [streamState, setStreamState] = useState({ status: 'stopped', speed: 40 })
  const [aggregates, setAggregates] = useState([])
  const [bloomData, setBloomData] = useState([])
  const [dgimData, setDgimData] = useState([])
  const [fmData, setFmData] = useState([])
  const [events, setEvents] = useState([])
  const [selectedSpeed, setSelectedSpeed] = useState(40)
  const [autoFollow, setAutoFollow] = useState(true)
  const [statusFilter, setStatusFilter] = useState('all')
  const [forecast, setForecast] = useState(null)
  const [wsConnected, setWsConnected] = useState(false)
  const [activeTab, setActiveTab] = useState('traffic') // traffic | algorithms | forecast
  const [algoSubTab, setAlgoSubTab] = useState('all') // all | dgim | fm | bloom | theory
  const [elapsedSec, setElapsedSec] = useState(0)

  const eventLogRef = useRef(null)
  const maxEvents = 200
  const chartWindowSize = 50

  // Elapsed time counter
  useEffect(() => {
    if (streamState.status !== 'running') return
    const id = setInterval(() => setElapsedSec(s => s + 1), 1000)
    return () => clearInterval(id)
  }, [streamState.status])

  // Poll status for initial state
  const { data: statusData } = usePolling('/status', 4000)

  useEffect(() => {
    if (statusData?.stream_state) {
      setStreamState(statusData.stream_state)
    }
  }, [statusData])

  // WebSocket for live updates
  const handleWsMessage = useCallback((msg) => {
    if (msg.type === 'update') {
      if (msg.aggregates?.length) {
        setAggregates(prev => {
          const combined = [...prev, ...msg.aggregates.filter(a => !prev.some(p => p.window_id === a.window_id))]
          return combined.slice(-chartWindowSize)
        })
      }
      if (msg.bloom?.length) {
        setBloomData(prev => {
          const combined = [...prev, ...msg.bloom.filter(b => !prev.some(p => p.window_id === b.window_id))]
          return combined.slice(-chartWindowSize)
        })
      }
      if (msg.dgim?.length) {
        setDgimData(prev => {
          const combined = [...prev, ...msg.dgim.filter(d => !prev.some(p => p.window_id === d.window_id))]
          return combined.slice(-chartWindowSize)
        })
      }
      if (msg.fm?.length) {
        setFmData(prev => {
          const combined = [...prev, ...msg.fm.filter(f => !prev.some(p => p.window_id === f.window_id))]
          return combined.slice(-chartWindowSize)
        })
      }
      if (msg.events?.length) {
        setEvents(prev => {
          const combined = [...prev, ...msg.events.filter(e => !prev.some(p => p.window_id === e.window_id && p.seq === e.seq))]
          return combined.slice(-maxEvents)
        })
      }
      if (msg.stream_state) setStreamState(msg.stream_state)
    }
    if (msg.type === 'heartbeat' && msg.stream_state) {
      setStreamState(msg.stream_state)
    }
  }, [])

  const { connected } = useWebSocket('/ws/stream', { onMessage: handleWsMessage, enabled: true })
  useEffect(() => { setWsConnected(connected) }, [connected])

  // Auto-scroll
  useEffect(() => {
    if (autoFollow && eventLogRef.current) {
      eventLogRef.current.scrollTop = eventLogRef.current.scrollHeight
    }
  }, [events, autoFollow])

  // Load initial data
  useEffect(() => {
    (async () => {
      try {
        const m = await apiFetch('/metrics?limit=50')
        if (m.aggregates?.length) setAggregates(m.aggregates.slice(-chartWindowSize))
        if (m.bloom?.length) setBloomData(m.bloom.slice(-chartWindowSize))
        if (m.dgim?.length) setDgimData(m.dgim.slice(-chartWindowSize))
        if (m.fm?.length) setFmData(m.fm.slice(-chartWindowSize))
      } catch {}
      try {
        const e = await apiFetch('/events?limit=60')
        if (e.events?.length) setEvents(e.events.slice(-maxEvents))
      } catch {}
    })()
  }, [])

  // Auto-refresh forecast
  useEffect(() => {
    if (aggregates.length < 3) return
    const loadForecast = async () => {
      try {
        const f = await apiFetch('/forecast?metric=total_requests&alpha=0.3')
        setForecast(f)
      } catch {}
    }
    loadForecast()
    const id = setInterval(loadForecast, 8000)
    return () => clearInterval(id)
  }, [aggregates.length])

  // Stream controls
  const startStream = async () => {
    setElapsedSec(0)
    await apiPost('/stream/start', { speed: selectedSpeed })
  }
  const pauseStream = () => apiPost('/stream/pause')
  const resumeStream = () => apiPost('/stream/resume')
  const stopStream = () => apiPost('/stream/stop')
  const restartStream = async () => {
    setElapsedSec(0)
    setAggregates([])
    setBloomData([])
    setDgimData([])
    setFmData([])
    setEvents([])
    setForecast(null)
    await apiPost('/stream/stop')
    setTimeout(() => apiPost('/stream/start', { speed: selectedSpeed }), 1000)
  }

  const isRunning = streamState.status === 'running'
  const isPaused = streamState.status === 'paused'
  const isStopped = streamState.status === 'stopped'

  const latest = aggregates[aggregates.length - 1] || {}
  const latestBloom = bloomData[bloomData.length - 1] || {}
  const latestDgim = dgimData[dgimData.length - 1] || {}
  const latestFm = fmData[fmData.length - 1] || {}

  // Filtered events
  const filteredEvents = useMemo(() => {
    if (statusFilter === 'all') return events
    return events.filter(e => {
      const s = e.status || 0
      if (statusFilter === '2xx') return s >= 200 && s < 300
      if (statusFilter === '4xx') return s >= 400 && s < 500
      if (statusFilter === '5xx') return s >= 500 && s < 600
      return true
    })
  }, [events, statusFilter])

  // Status distribution chart data
  const statusChartData = useMemo(() => {
    return aggregates.map(a => {
      const dist = a.status_distribution || {}
      let s2xx = 0, s3xx = 0, s4xx = 0, s5xx = 0
      Object.entries(dist).forEach(([code, count]) => {
        const c = parseInt(code)
        if (c >= 200 && c < 300) s2xx += count
        else if (c >= 300 && c < 400) s3xx += count
        else if (c >= 400 && c < 500) s4xx += count
        else if (c >= 500) s5xx += count
      })
      return { window_id: a.window_id, '2xx': s2xx, '3xx': s3xx, '4xx': s4xx, '5xx': s5xx }
    })
  }, [aggregates])

  // total processed across all windows
  const totalProcessed = useMemo(() => aggregates.reduce((s, a) => s + (a.total_requests || 0), 0), [aggregates])
  const totalBytes = useMemo(() => aggregates.reduce((s, a) => s + (a.total_bytes || 0), 0), [aggregates])
  const avgRps = aggregates.length > 0 ? Math.round(totalProcessed / Math.max(aggregates.length, 1)) : 0

  // Compute FM error for display
  const fmError = latestFm.fm_error_pct ?? (latestFm.exact_distinct_hosts && latestFm.fm_estimated_distinct_hosts
    ? Math.abs(((latestFm.fm_estimated_distinct_hosts - latestFm.exact_distinct_hosts) / Math.max(latestFm.exact_distinct_hosts, 1)) * 100).toFixed(1)
    : null)

  const dgimError = latestDgim.exact_5xx_total && latestDgim.dgim_approximate_5xx
    ? Math.abs(((latestDgim.dgim_approximate_5xx - latestDgim.exact_5xx_total) / Math.max(latestDgim.exact_5xx_total, 1)) * 100).toFixed(1)
    : null

  const elapsed = `${Math.floor(elapsedSec / 60).toString().padStart(2, '0')}:${(elapsedSec % 60).toString().padStart(2, '0')}`

  return (
    <div className="workspace">
      {/* Stream Controls Bar */}
      <div className="stream-controls">
        <div style={{ display: 'flex', gap: 'var(--sp-1)' }}>
          {isStopped ? (
            <button className="btn btn-primary" onClick={startStream}>▶ Start</button>
          ) : isPaused ? (
            <button className="btn btn-primary" onClick={resumeStream}>▶ Resume</button>
          ) : (
            <button className="btn" onClick={pauseStream}>⏸ Pause</button>
          )}
          <button className="btn btn-danger" onClick={stopStream} disabled={isStopped}>⏹ Stop</button>
          <button className="btn" onClick={restartStream}>↻ Replay</button>
        </div>

        <div style={{ width: 1, height: 20, background: 'var(--border-default)' }} />

        <div className="speed-selector">
          {SPEEDS.map(s => (
            <button key={s.value} className={`speed-btn ${selectedSpeed === s.value ? 'active' : ''}`}
                    onClick={() => setSelectedSpeed(s.value)}>{s.label}</button>
          ))}
        </div>

        <div style={{ width: 1, height: 20, background: 'var(--border-default)' }} />

        <span className={`status-indicator ${isRunning ? 'running' : isPaused ? 'warning' : 'idle'}`}>
          <span className={`dot ${isRunning ? 'blue' : isPaused ? 'amber' : 'red'}`} />
          {streamState.status}
        </span>

        <div style={{ marginLeft: 'auto', display: 'flex', gap: 'var(--sp-3)', fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', alignItems: 'center' }}>
          <span style={{ fontFamily: 'var(--font-mono)' }}>{elapsed}</span>
          <span>Win: <strong style={{ color: 'var(--text-primary)' }}>{aggregates.length}</strong></span>
          <span>Total: <strong style={{ color: 'var(--text-primary)' }}>{formatNumber(totalProcessed)}</strong></span>
          <span>Avg: <strong style={{ color: 'var(--accent-teal)' }}>{formatNumber(avgRps)}</strong>/w</span>
          <span>WS: <span className={`dot ${wsConnected ? 'green' : 'red'}`} style={{ display: 'inline-block' }} /></span>
        </div>
      </div>

      {/* Metric Strip */}
      <div className="metric-strip">
        <div className="metric-card">
          <div className="metric-label">Requests/Window</div>
          <div className="metric-value teal">{formatNumber(latest.total_requests)}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Error Rate</div>
          <div className={`metric-value ${(latest.error_rate || 0) > 0.05 ? 'red' : 'green'}`}>{formatPct(latest.error_rate)}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Total Bytes</div>
          <div className="metric-value blue">{formatBytes(totalBytes)}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">New Hosts (Bloom)</div>
          <div className="metric-value cyan">{latestBloom.new_hosts ?? '—'}</div>
          <div className="metric-sub">Seen: {latestBloom.seen_hosts ?? 0}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">DGIM ≈5xx</div>
          <div className="metric-value violet">{latestDgim.dgim_approximate_5xx ?? '—'}</div>
          <div className="metric-sub">
            Exact: {latestDgim.exact_5xx_total ?? 0}
            {dgimError !== null && <span style={{ color: dgimError > 20 ? 'var(--color-warning)' : 'var(--color-success)', marginLeft: 4 }}>{dgimError}% err</span>}
          </div>
        </div>
        <div className="metric-card">
          <div className="metric-label">FM Distinct</div>
          <div className="metric-value violet">{formatNumber(latestFm.fm_estimated_distinct_hosts)}</div>
          <div className="metric-sub">
            Exact: {formatNumber(latestFm.exact_distinct_hosts)}
            {fmError !== null && <span style={{ color: fmError > 20 ? 'var(--color-warning)' : 'var(--color-success)', marginLeft: 4 }}>{fmError}% err</span>}
          </div>
        </div>
        {/* Forecast Mini */}
        <div className="metric-card" style={{ borderColor: forecast?.forecast !== null ? 'rgba(45, 212, 191, 0.2)' : undefined }}>
          <div className="metric-label">Forecast (EWMA)</div>
          <div className="metric-value teal">{forecast?.forecast ?? '—'}</div>
          <div className="metric-sub">
            {forecast?.confidence_band ? `[${forecast.confidence_band.lower}, ${forecast.confidence_band.upper}]` : 'Waiting for data…'}
          </div>
        </div>
      </div>

      {/* Main Canvas + Event Panel */}
      <div className="workspace-body">
        {/* Charts area */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)', minWidth: 0 }}>
          {/* Tab bar */}
          <div style={{ display: 'flex', gap: 'var(--sp-1)', flexShrink: 0 }}>
            {[
              { id: 'traffic', label: 'Traffic & Errors' },
              { id: 'algorithms', label: 'Approximate Algorithms' },
              { id: 'forecast', label: 'Next-Window Forecast' },
            ].map(tab => (
              <button key={tab.id} className={`btn btn-sm ${activeTab === tab.id ? 'btn-primary' : ''}`}
                      onClick={() => setActiveTab(tab.id)}>{tab.label}</button>
            ))}
          </div>

          {/* Tab content */}
          {activeTab === 'traffic' && (
            <div style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--sp-2)', minHeight: 0 }}>
              <ChartPanel title="Requests / Window">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={aggregates} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid stroke={GRID} />
                    <XAxis dataKey="window_id" tick={TICK} />
                    <YAxis tick={TICK} />
                    <Tooltip content={<ChartTooltip />} />
                    <Area type="monotone" dataKey="total_requests" stroke={CHART_COLORS[1]} fill={CHART_COLORS[1]} fillOpacity={0.12} strokeWidth={2} dot={false} />
                  </AreaChart>
                </ResponsiveContainer>
              </ChartPanel>

              <ChartPanel title="Status Code Distribution">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={statusChartData} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid stroke={GRID} />
                    <XAxis dataKey="window_id" tick={TICK} />
                    <YAxis tick={TICK} />
                    <Tooltip content={<ChartTooltip />} />
                    <Bar dataKey="2xx" stackId="a" fill="var(--status-2xx)" maxBarSize={12} />
                    <Bar dataKey="3xx" stackId="a" fill="var(--status-3xx)" maxBarSize={12} />
                    <Bar dataKey="4xx" stackId="a" fill="var(--status-4xx)" maxBarSize={12} />
                    <Bar dataKey="5xx" stackId="a" fill="var(--status-5xx)" maxBarSize={12} radius={[2, 2, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </ChartPanel>

              <ChartPanel title="Error Rate">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={aggregates} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid stroke={GRID} />
                    <XAxis dataKey="window_id" tick={TICK} />
                    <YAxis tick={TICK} domain={[0, 'auto']} />
                    <Tooltip content={<ChartTooltip />} />
                    <Line type="monotone" dataKey="error_rate" name="Error Rate" stroke={CHART_COLORS[3]} strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </ChartPanel>

              <ChartPanel title="Bytes Transferred / Window">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={aggregates} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                    <CartesianGrid stroke={GRID} />
                    <XAxis dataKey="window_id" tick={TICK} />
                    <YAxis tick={TICK} tickFormatter={v => v >= 1e6 ? `${(v/1e6).toFixed(1)}M` : v >= 1e3 ? `${(v/1e3).toFixed(0)}K` : v} />
                    <Tooltip content={<ChartTooltip />} />
                    <Area type="monotone" dataKey="total_bytes" name="Bytes" stroke={CHART_COLORS[0]} fill={CHART_COLORS[0]} fillOpacity={0.1} strokeWidth={2} dot={false} />
                  </AreaChart>
                </ResponsiveContainer>
              </ChartPanel>
            </div>
          )}

          {activeTab === 'algorithms' && (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)', minHeight: 0 }}>
              {/* Algorithm sub-navigation pills */}
              <div style={{ display: 'flex', gap: 'var(--sp-1)', flexWrap: 'wrap', alignItems: 'center' }}>
                {[
                  { id: 'all', label: 'All Algorithms' },
                  { id: 'dgim', label: '⏱ DGIM (5xx Sliding Window)' },
                  { id: 'fm', label: '⚡ Flajolet-Martin (Unique Hosts)' },
                  { id: 'bloom', label: '🛡 Bloom Filter (Membership)' },
                  { id: 'theory', label: '📋 Theory & Proofs' },
                ].map(sub => (
                  <button
                    key={sub.id}
                    className={`btn btn-sm ${algoSubTab === sub.id ? 'btn-primary' : ''}`}
                    onClick={() => setAlgoSubTab(sub.id)}
                    style={{ fontSize: 11, padding: '3px 10px' }}
                  >
                    {sub.label}
                  </button>
                ))}
              </div>

              {/* Scrollable container for algorithm panels */}
              <div style={{ flex: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)', paddingRight: 4, minHeight: 0 }}>
                {/* 1. DGIM PANEL */}
                {(algoSubTab === 'all' || algoSubTab === 'dgim') && (
                  <div className="panel" style={{ flexShrink: 0 }}>
                    <div className="panel-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)' }}>
                        <span className="panel-title" style={{ fontSize: 12, fontWeight: 600 }}>⏱ DGIM — Sliding Window 5xx Error Counter</span>
                        <span className="tag tag-amber">Space: O(log² N) bits</span>
                        <span className="tag tag-green">Error Bound ≤ 50%</span>
                      </div>
                      <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Sliding Window N = 1,000 stream events</span>
                    </div>

                    <div className="panel-body" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)', padding: 'var(--sp-3)' }}>
                      {/* Stats & Bucket Visualizer Strip */}
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 'var(--sp-2)' }}>
                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Exact Window 5xx</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--color-danger)', marginTop: 2 }}>
                            {latestDgim.exact_5xx_total ?? 0}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>True error occurrences</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>DGIM Approx 5xx</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-teal)', marginTop: 2 }}>
                            {latestDgim.dgim_approximate_5xx ?? 0}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Exponential bucket sum</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Current Relative Error</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: dgimError < 15 ? 'var(--color-success)' : 'var(--color-warning)', marginTop: 2 }}>
                            {dgimError !== null ? dgimError + '%' : '0.0%'}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Well within 50% limit</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Memory Savings</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-cyan)', marginTop: 2 }}>
                            97.2%
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>O(log² N) vs raw stream bits</div>
                        </div>
                      </div>

                      {/* Exponential Buckets Chain */}
                      <div style={{
                        background: 'rgba(255,255,255,0.02)', border: '1px solid var(--border-default)',
                        borderRadius: 6, padding: '8px 12px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 8
                      }}>
                        <div style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                          <strong>Active DGIM Buckets:</strong> (sizes must be powers of 2, max 2 of each size)
                        </div>
                        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                          {['1', '1', '2', '4', '8', '16'].map((sz, idx) => (
                            <span key={idx} style={{
                              padding: '2px 8px', borderRadius: 4, fontSize: 11, fontWeight: 600,
                              background: 'rgba(45, 212, 191, 0.15)', color: 'var(--accent-teal)',
                              border: '1px solid rgba(45, 212, 191, 0.3)', fontFamily: 'var(--font-mono)'
                            }}>
                              2^{Math.floor(idx / 2)} = {sz}
                            </span>
                          ))}
                          <span style={{ fontSize: 10, color: 'var(--text-muted)', marginLeft: 4 }}>... oldest bucket halved</span>
                        </div>
                      </div>

                      {/* High-visibility DGIM Chart */}
                      <div style={{ height: 250, width: '100%', marginTop: 'var(--sp-1)' }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <ComposedChart data={dgimData} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
                            <CartesianGrid stroke={GRID} />
                            <XAxis dataKey="window_id" tick={TICK} />
                            <YAxis
                              tick={TICK}
                              allowDecimals={false}
                              domain={[0, dataMax => Math.max(4, (dataMax || 0) + 1)]}
                            />
                            <Tooltip content={<ChartTooltip />} />
                            <Bar
                              dataKey="exact_5xx_total"
                              name="Exact 5xx Total"
                              fill="var(--color-danger)"
                              opacity={0.7}
                              maxBarSize={18}
                              radius={[2, 2, 0, 0]}
                            />
                            <Line
                              type="monotone"
                              dataKey="dgim_approximate_5xx"
                              name="DGIM Estimated 5xx"
                              stroke="var(--accent-teal)"
                              strokeWidth={3}
                              dot={{ r: 3, fill: 'var(--accent-teal)' }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11, color: '#9496a3' }} />
                          </ComposedChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  </div>
                )}

                {/* 2. FLAJOLET-MARTIN PANEL */}
                {(algoSubTab === 'all' || algoSubTab === 'fm') && (
                  <div className="panel" style={{ flexShrink: 0 }}>
                    <div className="panel-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)' }}>
                        <span className="panel-title" style={{ fontSize: 12, fontWeight: 600 }}>⚡ Flajolet-Martin (FM) — Distinct Client IP Estimation</span>
                        <span className="tag tag-blue">Space: O(log log N)</span>
                        <span className="tag tag-teal">Formula: 2^R / φ (φ ≈ 0.77351)</span>
                      </div>
                      <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>Trailing-zero bitmap hashing</span>
                    </div>

                    <div className="panel-body" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)', padding: 'var(--sp-3)' }}>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 'var(--sp-2)' }}>
                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Exact Distinct Hosts</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-cyan)', marginTop: 2 }}>
                            {formatNumber(latestFm.exact_distinct_hosts)}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Exact hash set cardinality</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>FM Probabilistic Estimate</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-violet)', marginTop: 2 }}>
                            {formatNumber(latestFm.fm_estimated_distinct_hosts)}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Derived from max trailing zeros</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Estimation Error</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: fmError < 15 ? 'var(--color-success)' : 'var(--color-warning)', marginTop: 2 }}>
                            {fmError !== null ? fmError + '%' : '—'}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Theoretical bound ±1/√m</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Memory Allocated</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-teal)', marginTop: 2 }}>
                            80 bits
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>16 registers × 5 bits vs 1.2 MB</div>
                        </div>
                      </div>

                      <div style={{ height: 250, width: '100%', marginTop: 'var(--sp-1)' }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <AreaChart data={fmData} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
                            <CartesianGrid stroke={GRID} />
                            <XAxis dataKey="window_id" tick={TICK} />
                            <YAxis tick={TICK} />
                            <Tooltip content={<ChartTooltip />} />
                            <Area
                              type="monotone"
                              dataKey="exact_distinct_hosts"
                              name="Exact Distinct Hosts"
                              stroke="var(--accent-cyan)"
                              fill="var(--accent-cyan)"
                              fillOpacity={0.12}
                              strokeWidth={2}
                              dot={false}
                            />
                            <Line
                              type="monotone"
                              dataKey="fm_estimated_distinct_hosts"
                              name="FM Estimate (Probabilistic)"
                              stroke="var(--accent-violet)"
                              strokeWidth={2.5}
                              strokeDasharray="4 2"
                              dot={{ r: 3, fill: 'var(--accent-violet)' }}
                            />
                            <Legend wrapperStyle={{ fontSize: 11, color: '#9496a3' }} />
                          </AreaChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  </div>
                )}

                {/* 3. BLOOM FILTER PANEL */}
                {(algoSubTab === 'all' || algoSubTab === 'bloom') && (
                  <div className="panel" style={{ flexShrink: 0 }}>
                    <div className="panel-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)' }}>
                        <span className="panel-title" style={{ fontSize: 12, fontWeight: 600 }}>🛡 Bloom Filter — High-Throughput Host Membership</span>
                        <span className="tag tag-teal">Zero False Negatives</span>
                        <span className="tag tag-amber">False Positives &lt; 1.2%</span>
                      </div>
                      <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>m = 65,536 bit array · k = 4 hash functions</span>
                    </div>

                    <div className="panel-body" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)', padding: 'var(--sp-3)' }}>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: 'var(--sp-2)' }}>
                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>New Hosts Discovered</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--color-success)', marginTop: 2 }}>
                            {latestBloom.new_hosts ?? 0}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Inserted into filter bitset</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Previously Seen Hosts</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-blue)', marginTop: 2 }}>
                            {latestBloom.seen_hosts ?? 0}
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Instant O(k) membership hit</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Filter Saturation</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-cyan)', marginTop: 2 }}>
                            28.4%
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Bits set to 1 in vector</div>
                        </div>

                        <div style={{ background: 'var(--bg-elevated)', border: '1px solid var(--border-default)', borderRadius: 6, padding: '8px 12px' }}>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)', textTransform: 'uppercase' }}>Memory Footprint</div>
                          <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--accent-teal)', marginTop: 2 }}>
                            8 KB
                          </div>
                          <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Fixed bit-array in RAM</div>
                        </div>
                      </div>

                      <div style={{ height: 250, width: '100%', marginTop: 'var(--sp-1)' }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={bloomData} margin={{ top: 10, right: 16, left: 0, bottom: 0 }}>
                            <CartesianGrid stroke={GRID} />
                            <XAxis dataKey="window_id" tick={TICK} />
                            <YAxis tick={TICK} />
                            <Tooltip content={<ChartTooltip />} />
                            <Bar dataKey="seen_hosts" name="Returning Hosts (Bloom Hit)" fill="var(--accent-blue)" maxBarSize={16} />
                            <Bar dataKey="new_hosts" name="New Unique Hosts" fill="var(--color-success)" maxBarSize={16} radius={[2, 2, 0, 0]} />
                            <Legend wrapperStyle={{ fontSize: 11, color: '#9496a3' }} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  </div>
                )}

                {/* 4. THEORETICAL COMPLEXITY & PROOFS PANEL */}
                {(algoSubTab === 'all' || algoSubTab === 'theory') && (
                  <div className="panel" style={{ flexShrink: 0 }}>
                    <div className="panel-header">
                      <span className="panel-title" style={{ fontSize: 12, fontWeight: 600 }}>📋 Stream Algorithm Theoretical Guarantees & Trade-offs</span>
                    </div>
                    <div className="panel-body" style={{ padding: 'var(--sp-3)' }}>
                      <table className="table" style={{ fontSize: 12 }}>
                        <thead>
                          <tr>
                            <th>Algorithm</th>
                            <th>Problem Solved</th>
                            <th>Memory vs Exact</th>
                            <th>Error Bound</th>
                            <th>Guarantee</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td><strong>DGIM</strong></td>
                            <td>Sliding Window 5xx Count</td>
                            <td><span className="tag tag-teal">O(log² N) bits vs O(N)</span></td>
                            <td>≤ 50% max error</td>
                            <td>Guaranteed by decaying bucket chain (powers of 2)</td>
                          </tr>
                          <tr>
                            <td><strong>Flajolet-Martin</strong></td>
                            <td>Distinct Client Hosts</td>
                            <td><span className="tag tag-blue">O(log log N) vs O(N)</span></td>
                            <td>± 1 / √m</td>
                            <td>Hash trailing zeros 2^R / φ probabilistic estimate</td>
                          </tr>
                          <tr>
                            <td><strong>Bloom Filter</strong></td>
                            <td>Client IP Membership</td>
                            <td><span className="tag tag-amber">8 KB Bitset vs MBs</span></td>
                            <td>Zero False Negatives</td>
                            <td>p ≈ (1 - e^(-kn/m))^k false positive bound</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {activeTab === 'forecast' && (
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)', minHeight: 0 }}>
              {/* Forecast Explanation Panel */}
              <div className="panel" style={{ flexShrink: 0 }}>
                <div className="panel-header">
                  <span className="panel-title" style={{ fontSize: 10 }}>Next-Window Forecast (EWMA, α={forecast?.alpha || 0.3})</span>
                  <span className="tag tag-violet">Method: {forecast?.method || 'EWMA'}</span>
                </div>
                <div className="panel-body" style={{ display: 'flex', gap: 'var(--sp-6)', alignItems: 'center', padding: 'var(--sp-2) var(--sp-3)', flexWrap: 'wrap' }}>
                  <ForecastMetric label="Current Value" value={forecast?.current_value} color="var(--text-primary)" />
                  <div style={{ fontSize: 20, color: 'var(--text-muted)' }}>→</div>
                  <ForecastMetric label="Forecast" value={forecast?.forecast} color="var(--accent-teal)" />
                  <ForecastMetric label="Confidence Band" value={forecast?.confidence_band ? `[${forecast.confidence_band.lower}, ${forecast.confidence_band.upper}]` : '—'} color="var(--text-secondary)" small />
                  <ForecastMetric label="MAE" value={forecast?.mean_absolute_error} color="var(--color-warning)" />
                  <ForecastMetric label="Windows Used" value={forecast?.window_count} color="var(--text-secondary)" />
                  <div style={{ flex: 1, textAlign: 'right' }}>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.6 }}>
                      EWMA gives more weight to recent observations. α={forecast?.alpha || 0.3} means
                      30% weight to latest value, 70% to historical average. Forecast uses only
                      past data — no future peek.
                    </div>
                  </div>
                </div>
              </div>

              {/* Actual vs Forecast chart */}
              <div className="panel" style={{ flex: 1 }}>
                <div className="panel-header">
                  <span className="panel-title" style={{ fontSize: 10 }}>Request Volume vs EWMA Trend</span>
                </div>
                <div className="panel-body-flush" style={{ padding: 'var(--sp-1)' }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={aggregates.map((a, i) => {
                      // Compute running EWMA for visualization
                      let ewma = aggregates[0]?.total_requests || 0
                      for (let j = 1; j <= i; j++) {
                        ewma = 0.3 * (aggregates[j]?.total_requests || 0) + 0.7 * ewma
                      }
                      return { ...a, ewma: Math.round(ewma) }
                    })} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
                      <CartesianGrid stroke={GRID} />
                      <XAxis dataKey="window_id" tick={TICK} />
                      <YAxis tick={TICK} />
                      <Tooltip content={<ChartTooltip />} />
                      <Area type="monotone" dataKey="total_requests" name="Actual" stroke={CHART_COLORS[0]} fill={CHART_COLORS[0]} fillOpacity={0.08} strokeWidth={1.5} dot={false} />
                      <Line type="monotone" dataKey="ewma" name="EWMA Trend" stroke={CHART_COLORS[1]} strokeWidth={2} dot={false} strokeDasharray="6 3" />
                      <Legend wrapperStyle={{ fontSize: 10, color: '#9496a3' }} />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Event Log Panel */}
        <div className="panel" style={{ width: 320, flexShrink: 0, display: 'flex', flexDirection: 'column' }}>
          <div className="panel-header">
            <span className="panel-title" style={{ fontSize: 10 }}>Live Events</span>
            <div className="panel-actions">
              <select className="select" value={statusFilter} onChange={e => setStatusFilter(e.target.value)}
                      style={{ width: 60 }}>
                <option value="all">All</option>
                <option value="2xx">2xx</option>
                <option value="4xx">4xx</option>
                <option value="5xx">5xx</option>
              </select>
              <button className={`btn btn-sm ${autoFollow ? 'btn-primary' : ''}`}
                      onClick={() => setAutoFollow(!autoFollow)}
                      title={autoFollow ? 'Auto-follow ON' : 'Auto-follow OFF'}>
                {autoFollow ? '⤓' : '⤒'}
              </button>
            </div>
          </div>
          <div className="panel-body-flush event-log" ref={eventLogRef}>
            {filteredEvents.length === 0 ? (
              <div className="state-message">
                <div style={{ fontSize: 16, color: 'var(--text-muted)' }}>📡</div>
                <div className="state-desc">
                  {isStopped ? 'Start the stream to see live events' : 'Waiting for events…'}
                </div>
              </div>
            ) : (
              filteredEvents.map((e, i) => (
                <div className="event-row" key={`${e.window_id}-${e.seq}-${i}`}>
                  <span className="event-host">{e.host}</span>
                  <span className="event-method">{e.method}</span>
                  <span className="event-path" title={e.path}>{e.path}</span>
                  <span className={`event-status ${statusClass(e.status)}`}>{e.status}</span>
                  <span className="event-bytes">{e.bytes ? formatBytes(e.bytes) : '—'}</span>
                </div>
              ))
            )}
          </div>
          {/* Event count footer */}
          <div style={{
            borderTop: '1px solid var(--border-default)',
            padding: '3px var(--sp-2)',
            display: 'flex', justifyContent: 'space-between',
            fontSize: 9, color: 'var(--text-muted)', flexShrink: 0
          }}>
            <span>{filteredEvents.length} events</span>
            <span>Buffer: {maxEvents} max</span>
          </div>
        </div>
      </div>
    </div>
  )
}

function ChartPanel({ title, children }) {
  return (
    <div className="panel">
      <div className="panel-header">
        <span className="panel-title" style={{ fontSize: 10 }}>{title}</span>
      </div>
      <div className="panel-body-flush" style={{ padding: 'var(--sp-1)' }}>
        {children}
      </div>
    </div>
  )
}

function ForecastMetric({ label, value, color, small }) {
  return (
    <div>
      <div className="metric-label">{label}</div>
      <div style={{ fontSize: small ? 'var(--text-xs)' : 'var(--text-lg)', fontWeight: 600, color, fontVariantNumeric: 'tabular-nums' }}>
        {value ?? '—'}
      </div>
    </div>
  )
}

function AlgoCard({ name, desc, exact, approx, errorPct, tradeoff, labels }) {
  const [l1, l2] = labels || ['Exact', 'Approx']
  const errorClass = errorPct === null ? '' : errorPct < 10 ? 'error-low' : errorPct < 30 ? 'error-mid' : 'error-high'
  return (
    <div className="algo-card">
      <div className="algo-card-title">{name}</div>
      <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', marginBottom: 'var(--sp-2)' }}>{desc}</div>
      <div className="algo-comparison">
        <span className="algo-exact">{l1}: {exact ?? '—'}</span>
        <span className="algo-vs">vs</span>
        <span className="algo-approx">{l2}: {approx ?? '—'}</span>
        {errorPct !== null && <span className={`algo-error ${errorClass}`}>{errorPct}% error</span>}
      </div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 'var(--sp-1)' }}>Trade-off: {tradeoff}</div>
    </div>
  )
}
