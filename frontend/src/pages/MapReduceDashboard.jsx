import { useState, useEffect, useCallback } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'

const STATUS_COLORS = {
  '2': '#34d399',
  '3': '#60a5fa',
  '4': '#fbbf24',
  '5': '#f87171',
}

export default function MapReduceDashboard() {
  const [dataset, setDataset] = useState(null)
  const [mrState, setMrState] = useState({ running: false, status: 'idle' })
  const [results, setResults] = useState(null)
  const [hdfsStatus, setHdfsStatus] = useState(null)

  const fetchData = useCallback(async () => {
    try {
      const db = await fetch('/api/dataset').then(r => r.json())
      setDataset(db)
      
      const hdfs = await fetch('/api/hdfs/status').then(r => r.json())
      setHdfsStatus(hdfs)
      
      const st = await fetch('/api/mapreduce/status').then(r => r.json())
      setMrState(st)
      
      if (st.status === 'completed') {
        const res = await fetch('/api/mapreduce/results').then(r => r.json())
        setResults(res)
      } else if (st.status === 'idle') {
        // May have historical results sitting there. Try fetching
        const res = await fetch('/api/mapreduce/results').then(r => r.json())
        if (res.available && Object.keys(res.results).length > 0) {
            setResults(res)
        }
      }
    } catch (e) {
      // API error
    }
  }, [])

  useEffect(() => {
    fetchData()
    const interval = setInterval(() => {
      fetch('/api/mapreduce/status')
        .then(r => r.json())
        .then(st => {
           setMrState(st)
           if (st.status === 'completed' && (!results || Object.keys(results.results).length === 0)) {
               fetchData() // Refresh everything once completed
           }
        })
    }, 3000)
    return () => clearInterval(interval)
  }, [fetchData, results])

  const runMapReduce = async () => {
    await fetch('/api/mapreduce/run', { method: 'POST' })
    fetchData()
  }

  // Format data for chart
  let chartData = []
  if (results && results.available && Object.keys(results.results).length > 0) {
    chartData = Object.entries(results.results)
      .map(([code, count]) => ({ code, count, fill: STATUS_COLORS[code[0]] || '#94a3b8' }))
      .sort((a, b) => b.count - a.count)
  }

  return (
    <div className="mr-dashboard view-fade-in">
      <header className="app-header">
        <div className="header-top">
          <div className="header-brand">
            <h1>📦 Batch MapReduce Analytics</h1>
            <p className="subtitle">Execute Hadoop Streaming jobs directly against HDFS datasets for full-batch aggregations.</p>
          </div>
          <div className="header-right">
             <span className={`live-pill ${mrState.running ? 'on' : ''}`}>
               {mrState.running ? 'JOB RUNNING' : 'IDLE'}
             </span>
          </div>
        </div>
      </header>

      <div className="three-col">
        {/* Dataset Info */}
        <div className="card">
          <h3 className="section-title">Raw Dataset</h3>
          <div className="stat-box" style={{ marginTop: '1rem' }}>
            <div className="stat-row">
              <span className="stat-label">File Available</span>
              <span className={`stat-val ${dataset?.available ? 'green' : 'red'}`}>
                 {dataset?.available ? 'Yes' : 'No'}
              </span>
            </div>
            <div className="stat-row">
              <span className="stat-label">Total Lines</span>
              <span className="stat-val">{dataset?.lines ? dataset.lines.toLocaleString() : '—'}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label">Size</span>
              <span className="stat-val">
                  {dataset?.size_bytes ? (dataset.size_bytes / 1024 / 1024).toFixed(2) + ' MB' : '—'}
              </span>
            </div>
          </div>
        </div>

        {/* HDFS Context */}
        <div className="card">
          <h3 className="section-title">HDFS Storage Subsystem</h3>
          <div className="stat-box" style={{ marginTop: '1rem' }}>
             <div className="stat-row">
               <span className="stat-label">HDFS Status</span>
               <span className={`stat-val ${hdfsStatus?.available ? 'cyan' : 'red'}`}>
                  {hdfsStatus?.available ? 'Online' : 'Offline'}
               </span>
             </div>
             {hdfsStatus?.available && (
                <div style={{ marginTop: '0.5rem', fontSize: '0.75rem', color: 'var(--text-muted)' }}>
                    <strong>Directories in /user/data/:</strong>
                    <ul style={{ marginTop: '0.2rem', paddingLeft: '1rem', fontFamily: 'monospace' }}>
                        {hdfsStatus.files.map(f => (
                            <li key={f.path}>{f.path}</li>
                        ))}
                    </ul>
                </div>
             )}
          </div>
        </div>

        {/* Job Control */}
        <div className="card">
          <h3 className="section-title">MapReduce Execution</h3>
          <p className="chart-help" style={{ marginTop: '1rem' }}>
            Initiates a fully distributed batch workload.
            <br/><br/>
            <strong>Pipeline:</strong><br/>
            Wait for HDFS copy → Map → Shuffle/Sort → Reduce → Store in HDFS
          </p>
          
          <button 
             className="btn primary" 
             style={{ width: '100%', marginTop: '1rem', padding: '0.8rem' }}
             disabled={!hdfsStatus?.available || !dataset?.available || mrState.running}
             onClick={runMapReduce}
          >
             {mrState.running ? '⏳ MapReduce Job Running...' : '🚀 Start Batch Job'}
          </button>
          {mrState.status === 'failed' && (
             <div style={{ color: 'var(--red)', fontSize: '0.8rem', marginTop: '0.5rem', textAlign: 'center' }}>
                Job failed. Check docker logs.
             </div>
          )}
        </div>
      </div>

      {/* Results View */}
      {chartData.length > 0 && (
          <div className="card view-fade-in" style={{ marginTop: '1rem' }}>
             <h3 className="section-title">Batch Results: HTTP Status Code Frequency</h3>
             <p className="chart-help">Exact historical counts of all status codes generated by the Reducer phase.</p>
             
             <div style={{ display: 'grid', gridTemplateColumns: '8fr 2fr', gap: '2rem', marginTop: '1.5rem' }}>
                <div className="chart-box mid">
                   <ResponsiveContainer>
                      <BarChart data={chartData} margin={{ top: 10, right: 10, left: 20, bottom: 0 }}>
                         <XAxis dataKey="code" tick={{ fill: '#8b95a8', fontSize: 11 }} axisLine={false} tickLine={false} />
                         <YAxis tick={{ fill: '#8b95a8', fontSize: 11 }} axisLine={false} tickLine={false} tickFormatter={v => v >= 1000000 ? `${(v/1000000).toFixed(1)}M` : v >= 1000 ? `${(v/1000).toFixed(0)}K` : v} />
                         <Tooltip cursor={{ fill: 'rgba(255,255,255,0.02)' }} contentStyle={{ backgroundColor: 'rgba(14, 18, 27, 0.95)', border: 'none', borderRadius: '8px', color: '#fff'}} />
                         <Bar dataKey="count" radius={[4, 4, 0, 0]} isAnimationActive={true}>
                           {chartData.map((d) => (
                             <Cell key={d.code} fill={d.fill} />
                           ))}
                         </Bar>
                      </BarChart>
                   </ResponsiveContainer>
                </div>

                <div className="stat-box" style={{ alignSelf: 'start' }}>
                   {chartData.map(d => (
                       <div className="stat-row" key={d.code}>
                          <span className="stat-label">Code {d.code}</span>
                          <span className="stat-val" style={{ color: d.fill }}>{d.count.toLocaleString()}</span>
                       </div>
                   ))}
                   <div className="stat-row" style={{ borderTop: '2px dashed var(--border)', marginTop: '0.5rem', paddingTop: '0.5rem' }}>
                      <span className="stat-label"><strong>Total</strong></span>
                      <span className="stat-val"><strong>{results.total.toLocaleString()}</strong></span>
                   </div>
                </div>
             </div>
          </div>
      )}
    </div>
  )
}
