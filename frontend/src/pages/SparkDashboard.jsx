import { useState, useEffect, useCallback } from 'react'
import Header from '../components/Header'
import Overview from '../components/Overview'
import DataStream from '../components/DataStream'
import CoreTraffic from '../components/CoreTraffic'
import StatusBreakdown from '../components/StatusBreakdown'
import StreamMining from '../components/StreamMining'

// Refactored DataFlowPipeline for Spark context
function DataFlowPipeline({ status, metrics }) {
  const isLive = status.running
  const totalReqs = metrics.aggregates?.reduce((s, a) => s + (a.total_requests || 0), 0) || 0

  const nodes = [
    { icon: '📦', label: 'Dataset', stat: 'ClarkNet', active: true },
    { icon: '🔄', label: 'Simulator', stat: isLive ? 'streaming' : 'idle', active: status.demo?.replayer },
    { icon: '⚡', label: 'Spark Streaming', stat: 'windowed', active: status.demo?.stream },
    { icon: '🗄️', label: 'MongoDB', stat: status.mongo ? 'connected' : 'offline', active: status.mongo },
    { icon: '📊', label: 'Dashboard', stat: `${totalReqs.toLocaleString()} reqs`, active: true },
  ]

  return (
    <section className="data-flow-section card">
      <h3 className="section-title">Spark Streaming Pipeline</h3>
      <div className="flow-pipeline">
        {nodes.map((node, i) => (
          <span key={node.label} style={{ display: 'contents' }}>
            <div className={`flow-node ${node.active ? 'active' : ''}`}>
              <span className="flow-node-icon">{node.icon}</span>
              <span className="flow-node-label">{node.label}</span>
              <span className="flow-node-stat">{node.stat}</span>
            </div>
            {i < nodes.length - 1 && (
              <div className={`flow-arrow ${isLive ? 'active' : ''}`} />
            )}
          </span>
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
        fetch('/api/events?limit=48').then((r) => r.json()),
      ])
      setStatus(statusData)
      setMetrics(metricsData)
      setEvents(eventsData.events || [])
    } catch (e) {
      // ignore
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

  return (
    <div className="spark-dashboard view-fade-in">
      <Header 
        status={status} 
        busy={busy} 
        onStart={(resume) => toggleDemo('start', resume)} 
        onStop={() => toggleDemo('stop')} 
      />
      
      <DataFlowPipeline status={status} metrics={metrics} />
      
      <div className="dashboard-grid">
        <Overview metrics={metrics} live={status.running} />
      </div>

      <div className="two-col" style={{ display: 'grid', gridTemplateColumns: '1fr 3fr', gap: '1rem', marginBottom: '1rem' }}>
          <DataStream events={events} live={status.running} />
          <CoreTraffic aggregates={metrics.aggregates} />
      </div>

      <StreamMining bloom={metrics.bloom} dgim={metrics.dgim} fm={metrics.fm} />
      <StatusBreakdown aggregates={metrics.aggregates} />
    </div>
  )
}
