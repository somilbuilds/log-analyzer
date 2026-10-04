import { Link } from 'react-router-dom'
import { useEffect, useMemo, useState } from 'react'

function compactNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return Number(value).toLocaleString()
}

function bytes(value) {
  if (!value) return '—'
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)} GB`
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)} MB`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)} KB`
  return `${value} B`
}

function StatusCell({ label, state, detail, tone = 'green' }) {
  return (
    <div className="status-cell">
      <span className={`status-led ${state ? tone : 'muted'}`} />
      <div>
        <strong>{label}</strong>
        <small>{detail}</small>
      </div>
    </div>
  )
}

function PipelineRail({ title, stages, accent }) {
  return (
    <section className="ops-panel">
      <div className="panel-heading">
        <h2>{title}</h2>
      </div>
      <div className="pipeline-rail">
        {stages.map((stage, index) => (
          <div className="pipeline-stage-wrap" key={stage.label}>
            <div className={`pipeline-stage ${stage.active ? 'active' : ''}`} style={{ '--stage-accent': accent }}>
              <span className="stage-label">{stage.label}</span>
              <strong>{stage.value}</strong>
              <small>{stage.detail}</small>
            </div>
            {index < stages.length - 1 && <div className="pipeline-connector" />}
          </div>
        ))}
      </div>
    </section>
  )
}

export default function Home() {
  const [health, setHealth] = useState(null)
  const [status, setStatus] = useState(null)
  const [dataset, setDataset] = useState(null)
  const [hdfsStatus, setHdfsStatus] = useState(null)
  const [mrState, setMrState] = useState(null)
  const [metrics, setMetrics] = useState({ aggregates: [], bloom: [], dgim: [], fm: [] })

  useEffect(() => {
    let cancelled = false
    async function load() {
      try {
        const [healthData, statusData, datasetData, hdfsData, mrData, metricsData] = await Promise.all([
          fetch('/api/health').then((r) => r.json()).catch(() => ({ ok: false })),
          fetch('/api/status').then((r) => r.json()).catch(() => null),
          fetch('/api/dataset').then((r) => r.json()).catch(() => null),
          fetch('/api/hdfs/status').then((r) => r.json()).catch(() => null),
          fetch('/api/mapreduce/status').then((r) => r.json()).catch(() => null),
          fetch('/api/metrics?limit=80').then((r) => r.json()).catch(() => ({ aggregates: [], bloom: [], dgim: [], fm: [] })),
        ])
        if (!cancelled) {
          setHealth(healthData)
          setStatus(statusData)
          setDataset(datasetData)
          setHdfsStatus(hdfsData)
          setMrState(mrData)
          setMetrics(metricsData || { aggregates: [], bloom: [], dgim: [], fm: [] })
        }
      } catch {
        if (!cancelled) setHealth({ ok: false })
      }
    }
    load()
    const id = setInterval(load, 4000)
    return () => {
      cancelled = true
      clearInterval(id)
    }
  }, [])

  const summary = useMemo(() => {
    const aggregates = metrics.aggregates || []
    const last = aggregates[aggregates.length - 1] || {}
    const total = aggregates.reduce((sum, row) => sum + (row.total_requests || 0), 0)
    const rate = last.total_requests || 0
    const errorRate = (last.error_rate || 0) * 100
    const bloomLatest = metrics.bloom?.[metrics.bloom.length - 1] || {}
    return {
      total,
      rate,
      errorRate,
      window: status?.window_count || last.window_id || 0,
      hosts: bloomLatest.total_items_in_filter || 0,
      lastUpdate: status?.last_update ? status.last_update.split('T')[1]?.slice(0, 8) : '—',
    }
  }, [metrics, status])

  const streamingStages = [
    { label: 'ClarkNet', value: compactNumber(dataset?.lines || 3330000), detail: bytes(dataset?.size_bytes || 327500000), active: dataset?.available !== false },
    { label: 'Replayer', value: status?.running ? 'running' : 'idle', detail: `${summary.window} windows`, active: Boolean(status?.demo?.replayer || status?.running) },
    { label: 'Spark / streaming', value: status?.running ? `${compactNumber(summary.rate)} req/window` : 'waiting', detail: `last ${summary.lastUpdate}`, active: Boolean(status?.demo?.stream || status?.running) },
    { label: 'Bloom / DGIM / FM', value: `${compactNumber(summary.hosts)} hosts`, detail: `${summary.errorRate.toFixed(2)}% latest errors`, active: summary.total > 0 },
    { label: 'MongoDB', value: status?.mongo ? 'connected' : 'offline', detail: 'metrics store', active: Boolean(status?.mongo) },
  ]

  const batchStages = [
    { label: 'ClarkNet', value: dataset?.available ? 'available' : 'missing', detail: bytes(dataset?.size_bytes || 327500000), active: dataset?.available !== false },
    { label: 'HDFS', value: hdfsStatus?.available ? 'online' : 'offline', detail: `${hdfsStatus?.files?.length || 0} tracked paths`, active: Boolean(hdfsStatus?.available) },
    { label: 'Map', value: mrState?.running ? 'active' : 'ready', detail: 'parse status codes', active: Boolean(mrState?.running || mrState?.status === 'completed') },
    { label: 'Shuffle / sort', value: mrState?.status || 'idle', detail: 'group by code', active: Boolean(mrState?.running || mrState?.status === 'completed') },
    { label: 'Reduce', value: mrState?.status === 'completed' ? 'completed' : 'pending', detail: 'exact counts', active: mrState?.status === 'completed' },
  ]

  return (
    <div className="overview-page view-fade-in">
      <header className="page-header">
        <div>
          <p className="eyebrow">Big Data Analytics Console</p>
          <h1>ClarkNet Analytics</h1>
          <p className="subtitle">Real 1995 Internet traffic replayed through streaming algorithms and batch MapReduce jobs.</p>
        </div>
        <div className="header-actions">
          <Link className="btn primary" to="/spark">Open stream</Link>
          <Link className="btn batch" to="/mapreduce">Run batch</Link>
        </div>
      </header>

      <section className="metric-strip">
        <div className="metric-block">
          <span>Archive</span>
          <strong>{compactNumber(dataset?.lines || 3330000)}</strong>
          <small>ClarkNet requests</small>
        </div>
        <div className="metric-block">
          <span>Input size</span>
          <strong>{bytes(dataset?.size_bytes || 327500000)}</strong>
          <small>raw HTTP access logs</small>
        </div>
        <div className="metric-block">
          <span>Current window</span>
          <strong>W{summary.window || 0}</strong>
          <small>{compactNumber(summary.rate)} requests/window</small>
        </div>
        <div className="metric-block">
          <span>Unique hosts</span>
          <strong>{compactNumber(summary.hosts)}</strong>
          <small>Bloom filter items</small>
        </div>
        <div className="metric-block danger-metric">
          <span>Latest error rate</span>
          <strong>{summary.errorRate.toFixed(2)}%</strong>
          <small>4xx + 5xx responses</small>
        </div>
      </section>

      <section className="ops-grid two">
        <div className="ops-panel">
          <div className="panel-heading">
            <h2>System State</h2>
          </div>
          <div className="status-matrix">
            <StatusCell label="Backend API" state={health?.ok} detail={health ? (health.ok ? 'serving requests' : 'unavailable') : 'checking'} />
            <StatusCell label="Spark replay" state={status?.running} detail={status?.running ? 'historical logs streaming' : 'idle'} tone="blue" />
            <StatusCell label="MongoDB" state={status?.mongo} detail="streaming metrics store" />
            <StatusCell label="HDFS" state={hdfsStatus?.available} detail="batch storage layer" tone="purple" />
            <StatusCell label="MapReduce" state={mrState?.running || mrState?.status === 'completed'} detail={mrState?.status || 'idle'} tone="purple" />
          </div>
        </div>

        <div className="ops-panel">
          <div className="panel-heading">
            <h2>Recent Analytical Results</h2>
          </div>
          <div className="result-list">
            <div><span>Processed stream rows</span><strong>{compactNumber(summary.total)}</strong></div>
            <div><span>Latest micro-batch</span><strong>{compactNumber(summary.rate)} requests</strong></div>
            <div><span>Last update</span><strong>{summary.lastUpdate}</strong></div>
            <div><span>FM windows</span><strong>{compactNumber(metrics.fm?.length || 0)}</strong></div>
            <div><span>DGIM windows</span><strong>{compactNumber(metrics.dgim?.length || 0)}</strong></div>
          </div>
        </div>
      </section>

      <PipelineRail title="Streaming Path" stages={streamingStages} accent="var(--blue)" />
      <PipelineRail title="Batch Path" stages={batchStages} accent="var(--purple)" />
    </div>
  )
}
