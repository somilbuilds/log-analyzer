import { Link } from 'react-router-dom'
import { useState, useEffect } from 'react'

export default function Home() {
  const [health, setHealth] = useState(null)
  
  useEffect(() => {
    fetch('/api/health')
      .then(res => res.json())
      .then(data => setHealth(data))
      .catch(() => setHealth({ ok: false }))
  }, [])

  return (
    <div className="home-layout">
      <div className="home-container">
        
        <header className="home-header">
          <div className="home-badge">BDA LAB PROJECT</div>
          <h1 className="home-title">ClarkNet Log Analytics</h1>
          <p className="home-subtitle">
            A distributed computing demonstration contrasting batch and streaming paradigms over a shared historical dataset.
          </p>
        </header>

        <section className="home-architecture card">
          <h2 className="section-title">Architecture Overview</h2>
          <div className="arch-text">
            <p>
              This project demonstrates two distinct approaches to Big Data analytics using the <strong>1995 ClarkNet-HTTP traces</strong> (~1.6M requests).
            </p>
            <ul>
              <li><strong>HDFS:</strong> The foundational storage layer holding the raw logs.</li>
              <li><strong>Spark:</strong> Simulates a live data stream using Structural Streaming, windowed processing, and stream mining techniques (Bloom Filters, DGIM).</li>
              <li><strong>MapReduce:</strong> Computes historical aggregated counts of data natively in Hadoop via the MapReduce framework.</li>
            </ul>
          </div>
        </section>
        
        <div className="home-actions">
          <Link to="/spark" className="action-card spark-mode">
            <div className="action-icon">🌊</div>
            <h3>Live Stream Analytics</h3>
            <p>
              Replay historical data as an incoming stream and process it using <strong>Spark Structured Streaming</strong>. Includes windowed analytics, traffic monitoring, and algorithmic stream mining.
            </p>
            <span className="action-btn">Open Live Analytics →</span>
          </Link>

          <Link to="/mapreduce" className="action-card mr-mode">
            <div className="action-icon">📦</div>
            <h3>MapReduce Batch Analytics</h3>
            <p>
              Process the stored HDFS dataset as a distributed batch workload using <strong>Hadoop MapReduce</strong>. Includes job execution control and historical statistics.
            </p>
            <span className="action-btn">Open MapReduce Analytics →</span>
          </Link>
        </div>

        <footer className="home-footer">
          {health ? (
            <div className={`status-indicator ${health.ok ? 'ok' : 'error'}`}>
              <span className="dot"></span>
              {health.ok ? 'Backend APIs Online' : 'Backend Unavailable'}
            </div>
          ) : (
            <div className="status-indicator">Connecting...</div>
          )}
        </footer>
      </div>
    </div>
  )
}
