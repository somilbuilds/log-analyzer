import { useState, useEffect, useCallback, useMemo } from 'react'
import Header from './components/Header'
import Overview from './components/Overview'
import DataStream from './components/DataStream'
import CoreTraffic from './components/CoreTraffic'
import StatusBreakdown from './components/StatusBreakdown'
import StreamMining from './components/StreamMining'
import './index.css'

const TABS = [
  { id: 'traffic', label: '📈 Traffic & Anomalies' },
  { id: 'status', label: '📊 Status & Hosts' },
  { id: 'mining', label: '⚙️ Stream Mining' },
]

function DataFlowPipeline({ status, metrics }) {
  const isLive = status.running
  const totalReqs = metrics.aggregates?.reduce((s, a) => s + (a.total_requests || 0), 0) || 0

  const nodes = [
    { icon: '📦', label: 'ClarkNet', stat: '1.6M lines', active: true },
    { icon: '🔄', label: 'Replayer', stat: isLive ? 'active' : 'idle', active: status.demo?.replayer },
    { icon: '📂', label: 'HDFS', stat: 'Hadoop 3.2', active: true },
    { icon: '⚡', label: 'Spark', stat: 'streaming', active: status.demo?.stream },
    { icon: '🗄️', label: 'MongoDB', stat: status.mongo ? 'connected' : 'offline', active: status.mongo },
    { icon: '📊', label: 'Dashboard', stat: `${totalReqs.toLocaleString()} reqs`, active: true },
  ]

  return (
    <section className="data-flow-section card">
      <h3 className="section-title">Data pipeline</h3>
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

function App() {
  const [status, setStatus] = useState({
    running: false, last_update: null, window_count: 0, mongo: false,
    demo: { replayer: false, stream: false },
  })
  const [metrics, setMetrics] = useState({ aggregates: [], bloom: [], dgim: [], fm: [] })
  const [events, setEvents] = useState([])
  const [activeView, setActiveView] = useState('overview')
  const [autoRefresh, setAutoRefresh] = useState(true)
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
      /* API not reachable */
    }
  }, [])

  useEffect(() => {
    fetchData()
    if (!autoRefresh) return undefined
    const interval = setInterval(fetchData, 2000)
    return () => clearInterval(interval)
  }, [autoRefresh, fetchData])

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

  const VIEWS = [
    { id: 'overview', icon: '🏠', label: 'Overview & KPI' },
    { id: 'stream', icon: '🌊', label: 'Live Stream' },
    { id: 'traffic', icon: '📈', label: 'Traffic Anomalies' },
    { id: 'status', icon: '📊', label: 'Status & Hosts' },
    { id: 'mining', icon: '⚙️', label: 'Stream Mining' },
  ]

  return (
    <div className="layout-wrapper">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <span style={{ fontSize: '1.4rem' }}>⚡</span>
          <h2>ClarkNet AI</h2>
        </div>
        <nav className="sidebar-nav">
          {VIEWS.map(v => (
            <button
              key={v.id}
              className={`nav-item ${activeView === v.id ? 'active' : ''}`}
              onClick={() => setActiveView(v.id)}
            >
              <span className="nav-icon">{v.icon}</span>
              {v.label}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <label className="refresh-toggle">
            <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
            Auto refresh
          </label>
        </div>
      </aside>

      <div className="main-content">
        <Header 
          status={status} 
          busy={busy} 
          onStart={(resume) => toggleDemo('start', resume)} 
          onStop={() => toggleDemo('stop')} 
        />
        
        <div className="view-container">
          {activeView === 'overview' && (
            <div className="view-fade-in">
              <DataFlowPipeline status={status} metrics={metrics} />
              <Overview metrics={metrics} live={status.running} />
            </div>
          )}
          {activeView === 'stream' && (
            <div className="view-fade-in" style={{ height: '100%' }}>
              <DataStream events={events} live={status.running} />
            </div>
          )}
          {activeView === 'traffic' && (
            <div className="view-fade-in">
              <CoreTraffic aggregates={metrics.aggregates} />
            </div>
          )}
          {activeView === 'status' && (
            <div className="view-fade-in">
              <StatusBreakdown aggregates={metrics.aggregates} />
            </div>
          )}
          {activeView === 'mining' && (
            <div className="view-fade-in">
              <StreamMining bloom={metrics.bloom} dgim={metrics.dgim} fm={metrics.fm} />
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default App
