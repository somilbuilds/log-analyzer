import { useState, useEffect, useCallback, useMemo } from 'react'
import Header from '../components/Header'
import Overview from '../components/Overview'
import DataStream from '../components/DataStream'
import CoreTraffic from '../components/CoreTraffic'
import StatusBreakdown from '../components/StatusBreakdown'
import StreamMining from '../components/StreamMining'

function fmt(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—'
  return Number(value).toLocaleString()
}

function StreamingPipeline({ status, metrics }) {
  const aggregates = metrics.aggregates || []
  const last = aggregates[aggregates.length - 1] || {}
  const bloomLatest = metrics.bloom?.[metrics.bloom.length - 1] || {}
  const dgimLatest = metrics.dgim?.[metrics.dgim.length - 1] || {}
  const fmLatest = metrics.fm?.[metrics.fm.length - 1] || {}

  const stages = [
    { label: 'ClarkNet', value: '1995 logs', detail: 'historical HTTP archive', active: true },
    { label: 'Replayer', value: status.running ? 'running' : 'idle', detail: `${fmt(status.window_count || last.window_id || 0)} windows emitted`, active: status.running || status.demo?.replayer },
    { label: 'Spark / streaming', value: `${fmt(last.total_requests || 0)} req/window`, detail: `${((last.error_rate || 0) * 100).toFixed(2)}% latest errors`, active: status.running || status.demo?.stream },
    { label: 'Bloom / DGIM / FM', value: `${fmt(bloomLatest.total_items_in_filter || 0)} hosts`, detail: `DGIM ${fmt(dgimLatest.dgim_approximate_5xx || 0)} · FM ${fmt(fmLatest.fm_estimated_distinct_hosts || 0)}`, active: aggregates.length > 0 },
    { label: 'MongoDB', value: status.mongo ? 'connected' : 'offline', detail: 'window metrics persisted', active: status.mongo },
    { label: 'Dashboard', value: 'live view', detail: 'polling every 2s', active: true },
  ]

  return (
    <section className="ops-panel pipeline-panel">
      <div className="panel-heading">
        <h2>Streaming Pipeline State</h2>
        <p>ClarkNet → Replayer → Spark → stream-mining algorithms → MongoDB → dashboard.</p>
      </div>
      <div className="pipeline-rail horizontal">
        {stages.map((stage, index) => (
          <div className="pipeline-stage-wrap" key={stage.label}>
            <div className={`pipeline-stage ${stage.active ? 'active' : ''}`}>
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

export default function SparkDashboard() {
  const [status, setStatus] = useState({
    running: false, last_update: null, window_count: 0, mongo: false,
    demo: { replayer: false, stream: false },
  })
  const [metrics, setMetrics] = useState({ aggregates: [], bloom: [], dgim: [], fm: [] })
  const [events, setEvents] = useState([])
  const [busy, setBusy] = useState(false)

  const fetchData = useCallback(async () => {
    try {
      const [statusData, metricsData, eventsData] = await Promise.all([
        fetch('/api/status').then((r) => r.json()),
        fetch('/api/metrics?limit=160').then((r) => r.json()),
        fetch('/api/events?limit=80').then((r) => r.json()),
      ])
      setStatus(statusData)
      setMetrics(metricsData)
      setEvents(eventsData.events || [])
    } catch {
      // keep the last good console state visible
    }
  }, [])

  useEffect(() => {
    fetchData()
    const interval = setInterval(fetchData, 2000)
    return () => clearInterval(interval)
  }, [fetchData])

  const toggleDemo = async (action, resume = false) => {
    setBusy(true)
    try {
      const url = action === 'start' ? `/api/demo/start?resume=${resume}` : `/api/demo/${action}`
      await fetch(url, { method: 'POST' })
      await fetchData()
    } finally {
      setBusy(false)
    }
  }

  const latest = useMemo(() => {
    const aggregates = metrics.aggregates || []
    return aggregates[aggregates.length - 1] || {}
  }, [metrics])

  return (
    <div className="spark-dashboard view-fade-in">
      <Header
        status={status}
        busy={busy}
        onStart={(resume) => toggleDemo('start', resume)}
        onStop={() => toggleDemo('stop')}
      />

      <section className="metric-strip compact">
        <div className="metric-block">
          <span>Processing rate</span>
          <strong>{fmt(latest.total_requests || 0)}</strong>
          <small>requests in latest window</small>
        </div>
        <div className="metric-block">
          <span>Current window</span>
          <strong>W{status.window_count || latest.window_id || 0}</strong>
          <small>micro-batch position</small>
        </div>
        <div className="metric-block danger-metric">
          <span>Error rate</span>
          <strong>{(((latest.error_rate || 0) * 100)).toFixed(2)}%</strong>
          <small>4xx + 5xx latest window</small>
        </div>
        <div className="metric-block">
          <span>Unique hosts</span>
          <strong>{fmt(metrics.bloom?.[metrics.bloom.length - 1]?.total_items_in_filter || 0)}</strong>
          <small>Bloom filter items</small>
        </div>
      </section>

      <StreamingPipeline status={status} metrics={metrics} />
      <Overview metrics={metrics} live={status.running} />
      <DataStream events={events} live={status.running} />
      <CoreTraffic aggregates={metrics.aggregates} />
      <StreamMining bloom={metrics.bloom} dgim={metrics.dgim} fm={metrics.fm} />
      <StatusBreakdown aggregates={metrics.aggregates} />
    </div>
  )
}
