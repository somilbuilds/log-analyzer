import { BrowserRouter, Routes, Route, Link, useLocation } from 'react-router-dom'
import Home from './pages/Home'
import SparkDashboard from './pages/SparkDashboard'
import MapReduceDashboard from './pages/MapReduceDashboard'
import './index.css'

function Layout({ children }) {
  const location = useLocation()
  const isHome = location.pathname === '/'

  if (isHome) return children

  return (
    <div className="layout-wrapper">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <span style={{ fontSize: '1.4rem' }}>⚡</span>
          <h2>Log Analyzer</h2>
        </div>
        <nav className="sidebar-nav">
          <Link to="/" className="nav-item">
            <span className="nav-icon">🏠</span> Home
          </Link>
          <Link to="/spark" className={`nav-item ${location.pathname === '/spark' ? 'active' : ''}`}>
            <span className="nav-icon">🌊</span> Live Spark
          </Link>
          <Link to="/mapreduce" className={`nav-item ${location.pathname === '/mapreduce' ? 'active' : ''}`}>
            <span className="nav-icon">📦</span> Batch MapReduce
          </Link>
        </nav>
      </aside>
      <div className="main-content">
        {children}
      </div>
    </div>
  )
}

function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/spark" element={<SparkDashboard />} />
          <Route path="/mapreduce" element={<MapReduceDashboard />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  )
}

export default App
