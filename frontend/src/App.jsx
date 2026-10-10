import { BrowserRouter, Routes, Route, NavLink, useLocation } from 'react-router-dom'
import { useState, useEffect } from 'react'
import Overview from './pages/Overview'
import BatchWorkspace from './pages/BatchWorkspace'
import StreamWorkspace from './pages/StreamWorkspace'
import JobHistory from './pages/JobHistory'
import DatasetExplorer from './pages/DatasetExplorer'
import SystemHealth from './pages/SystemHealth'
import Settings from './pages/Settings'
import './index.css'

const NAV_ITEMS = [
  { path: '/', label: 'Overview', icon: '◈', end: true },
  { path: '/workspace-a', label: 'Batch Analytics', icon: '▣' },
  { path: '/workspace-b', label: 'Streaming Lab', icon: '◉' },
  { divider: true },
  { path: '/jobs', label: 'Job History', icon: '☰' },
  { path: '/dataset', label: 'Dataset', icon: '⬡' },
  { path: '/system', label: 'System Health', icon: '⚙' },
]

const PAGE_TITLES = {
  '/': 'Laboratory Overview',
  '/workspace-a': 'HDFS + MapReduce Batch Analytics',
  '/workspace-b': 'Spark Streaming Laboratory',
  '/jobs': 'Job History',
  '/dataset': 'Dataset & Storage Explorer',
  '/system': 'System Health & Diagnostics',
  '/settings': 'Settings',
}

function Clock() {
  const [time, setTime] = useState(new Date())
  useEffect(() => {
    const id = setInterval(() => setTime(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  return (
    <span className="topbar-clock">
      {time.toLocaleTimeString('en-US', { hour12: false })}
    </span>
  )
}

function TopBar({ health }) {
  const location = useLocation()
  const title = PAGE_TITLES[location.pathname] || 'ClarkNet Laboratory'

  return (
    <div className="topbar">
      <div className="topbar-left">
        <span className="topbar-title">{title}</span>
        <span className="topbar-breadcrumb">ClarkNet HTTP · 1995 Archive</span>
      </div>
      <div className="topbar-right">
        <div className="topbar-badge">
          <span className={`dot ${health?.hdfs ? 'green' : 'red'}`} />
          HDFS
        </div>
        <div className="topbar-badge">
          <span className={`dot ${health?.mongo ? 'green' : 'red'}`} />
          MongoDB
        </div>
        <div className="topbar-badge">
          <span className={`dot ${health?.ok ? 'green' : 'red'}`} />
          API
        </div>
        <Clock />
      </div>
    </div>
  )
}

function AppShell() {
  const [health, setHealth] = useState(null)

  useEffect(() => {
    const check = async () => {
      try {
        const r = await fetch('/api/health')
        setHealth(await r.json())
      } catch {
        setHealth({ ok: false, mongo: false, hdfs: false, dataset: false })
      }
    }
    check()
    const id = setInterval(check, 8000)
    return () => clearInterval(id)
  }, [])

  return (
    <div className="app-shell">
      <nav className="nav-rail" aria-label="Primary navigation">
        <div className="nav-brand">CN</div>
        {NAV_ITEMS.map((item, i) =>
          item.divider ? (
            <div className="nav-divider" key={`d${i}`} />
          ) : (
            <NavLink
              key={item.path}
              to={item.path}
              end={item.end}
              className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
            >
              <span className="nav-icon">{item.icon}</span>
              <span className="nav-label">{item.label}</span>
            </NavLink>
          )
        )}
        <div className="nav-spacer" />
        <NavLink
          to="/settings"
          className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
        >
          <span className="nav-icon">⚙</span>
          <span className="nav-label">Settings</span>
        </NavLink>
      </nav>

      <TopBar health={health} />

      <main className="main-content">
        <Routes>
          <Route path="/" element={<Overview health={health} />} />
          <Route path="/workspace-a" element={<BatchWorkspace />} />
          <Route path="/workspace-b" element={<StreamWorkspace />} />
          <Route path="/jobs" element={<JobHistory />} />
          <Route path="/dataset" element={<DatasetExplorer />} />
          <Route path="/system" element={<SystemHealth />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </main>
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <AppShell />
    </BrowserRouter>
  )
}
