import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import Home from './pages/Home'
import SparkDashboard from './pages/SparkDashboard'
import MapReduceDashboard from './pages/MapReduceDashboard'
import './index.css'

function Layout({ children }) {
  return (
    <div className="layout-wrapper">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="brand-mark">CN</div>
          <div>
            <h2>ClarkNet Analytics</h2>
            <p>1995 HTTP archive</p>
          </div>
        </div>

        <nav className="sidebar-nav" aria-label="Primary navigation">
          <NavLink to="/" end className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}>
            <span className="nav-icon">OV</span>
            Overview
          </NavLink>
          <NavLink to="/spark" className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}>
            <span className="nav-icon">ST</span>
            Streaming
          </NavLink>
          <NavLink to="/mapreduce" className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}>
            <span className="nav-icon">MR</span>
            Batch
          </NavLink>
        </nav>

        <div className="sidebar-meta">
          <span>Dataset</span>
          <strong>ClarkNet HTTP</strong>
          <small>3.33M requests · 327.5 MB</small>
        </div>
      </aside>

      <main className="main-content">{children}</main>
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
