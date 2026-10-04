import { useState, useEffect, useCallback, useMemo } from 'react'

const TASKS = [
  { id: 'status', label: 'HTTP status counts', resultLabel: 'Status code' },
  { id: 'hosts', label: 'Top hosts', resultLabel: 'Host' },
  { id: 'endpoints', label: 'Top endpoints', resultLabel: 'Endpoint' },
]

const STATUS_COLORS = { '2': '#31c48d', '3': '#4ea1ff', '4': '#f6c453', '5': '#f97373' }

function bytes(value) {
  if (!value) return '-'
  if (value >= 1_000_000_000) return `${(value / 1_000_000_000).toFixed(2)} GB`
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)} MB`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)} KB`
  return `${value} B`
}

function fmt(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '-'
  return Number(value).toLocaleString()
}

function StageCard({ label, value, detail, active, complete }) {
  return (
    <div className={`mr-stage ${active ? 'active' : ''} ${complete ? 'complete' : ''}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  )
}

export default function MapReduceDashboard() {
  const [dataset, setDataset] = useState(null)
  const [mrState, setMrState] = useState({ running: false, status: 'idle', task: 'status' })
  const [results, setResults] = useState(null)
  const [hdfsStatus, setHdfsStatus] = useState(null)
  const [selectedTask, setSelectedTask] = useState('status')
  const [busyUpload, setBusyUpload] = useState(false)

  const activeTask = mrState.running ? mrState.task : selectedTask
  const currentTask = TASKS.find((task) => task.id === activeTask) || TASKS[0]

  const fetchData = useCallback(async (task = selectedTask) => {
    try {
      const [db, hdfs, st] = await Promise.all([
        fetch('/api/dataset').then((r) => r.json()),
        fetch('/api/hdfs/status').then((r) => r.json()),
        fetch('/api/mapreduce/status').then((r) => r.json()),
      ])
      setDataset(db)
      setHdfsStatus(hdfs)
      setMrState(st)
      const res = await fetch(`/api/mapreduce/results?task=${task}`).then((r) => r.json()).catch(() => null)
      setResults(res?.task === task ? res : null)
    } catch {
      // leave the last readable state on screen
    }
  }, [selectedTask])

  useEffect(() => {
    fetchData(selectedTask)
    const interval = setInterval(() => fetchData(selectedTask), 3000)
    return () => clearInterval(interval)
  }, [fetchData, selectedTask])

  const uploadToHdfs = async () => {
    setBusyUpload(true)
    try {
      await fetch('/api/hdfs/upload', { method: 'POST' })
      await fetchData(selectedTask)
    } finally {
      setBusyUpload(false)
    }
  }

  const runMapReduce = async (task) => {
    setSelectedTask(task)
    await fetch(`/api/mapreduce/run?task=${task}`, { method: 'POST' })
    fetchData(task)
  }

  const rows = useMemo(() => {
    if (!results?.available) return []
    if (Array.isArray(results.rows) && results.rows.length) return results.rows
    return Object.entries(results.results || {}).map(([key, count]) => ({ key, count })).sort((a, b) => b.count - a.count)
  }, [results])

  const total = results?.total || rows.reduce((sum, row) => sum + Number(row.count || 0), 0)
  const isComplete = mrState.status === 'completed' && mrState.task === selectedTask
  const isRunning = mrState.running
  const canRun = hdfsStatus?.available && hdfsStatus?.dataset_ready && dataset?.available && !isRunning

  const stages = [
    { label: 'Dataset', value: dataset?.available ? 'available' : 'missing', detail: `${fmt(dataset?.lines)} rows`, active: true, complete: dataset?.available },
    { label: 'HDFS copy', value: hdfsStatus?.dataset_ready ? 'stored' : 'not copied', detail: '/user/data/raw_logs/access_log', active: hdfsStatus?.available, complete: hdfsStatus?.dataset_ready },
    { label: 'Map', value: isRunning ? 'processing' : isComplete ? 'complete' : 'pending', detail: currentTask.label, active: isRunning, complete: isComplete },
    { label: 'Shuffle / Sort', value: isRunning ? 'grouping' : isComplete ? 'complete' : 'pending', detail: 'sort key/value pairs', active: isRunning, complete: isComplete },
    { label: 'Reduce', value: isComplete ? 'complete' : 'pending', detail: 'aggregate counts', active: isRunning, complete: isComplete },
    { label: 'Results', value: rows.length ? 'available' : 'waiting', detail: `${fmt(total)} records counted`, active: rows.length > 0, complete: rows.length > 0 },
  ]

  const progress = rows.length ? 100 : isRunning ? 62 : hdfsStatus?.dataset_ready ? 34 : hdfsStatus?.available ? 20 : 8

  return (
    <div className="mr-dashboard view-fade-in">
      <header className="page-header batch-header">
        <div>
          <p className="eyebrow">Hadoop MapReduce</p>
          <h1>Batch Processing Console</h1>
          <p className="subtitle">Copy ClarkNet into HDFS once, then run Hadoop Streaming jobs over that stored dataset.</p>
        </div>
        <div className="stream-controls">
          <span className={`live-pill ${isRunning ? 'on' : ''}`}>{isRunning ? `Running ${mrState.task}` : mrState.status || 'Idle'}</span>
          <button className="btn" disabled={busyUpload || !dataset?.available} onClick={uploadToHdfs}>{busyUpload ? 'Uploading...' : 'Store dataset in HDFS'}</button>
        </div>
      </header>

      <section className="metric-strip compact">
        <div className="metric-block"><span>Input records</span><strong>{fmt(dataset?.lines)}</strong><small>raw ClarkNet rows</small></div>
        <div className="metric-block"><span>Input size</span><strong>{bytes(dataset?.size_bytes)}</strong><small>local dataset</small></div>
        <div className="metric-block"><span>HDFS dataset</span><strong>{hdfsStatus?.dataset_ready ? 'stored' : 'missing'}</strong><small>{hdfsStatus?.files?.length || 0} visible paths</small></div>
        <div className="metric-block"><span>Selected task</span><strong>{activeTask}</strong><small>{currentTask.label}</small></div>
        <div className="metric-block danger-metric"><span>Total counted</span><strong>{fmt(total)}</strong><small>latest selected result</small></div>
      </section>

      <section className="ops-panel mr-lifecycle">
        <div className="panel-heading"><h2>MapReduce Lifecycle</h2><p>Dataset to HDFS copy to Map to Shuffle / Sort to Reduce to Results.</p></div>
        <div className="progress-track"><div style={{ width: `${progress}%` }} /></div>
        <div className="mr-stage-grid">{stages.map((stage) => <StageCard key={stage.label} {...stage} />)}</div>
        {mrState.status === 'failed' && <div className="error-banner">MapReduce job failed. Check logs/mapreduce.log.</div>}
      </section>

      <section className="ops-panel">
        <div className="panel-heading"><h2>MapReduce Tasks</h2><p>Each button runs a real Hadoop Streaming mapper/reducer over /user/data/raw_logs/access_log in HDFS.</p></div>
        <div className="task-buttons">
          {TASKS.map((task) => (
            <button key={task.id} className={`btn batch ${selectedTask === task.id ? 'active' : ''}`} disabled={!canRun} onClick={() => runMapReduce(task.id)}>
              {isRunning && mrState.task === task.id ? 'Running...' : task.label}
            </button>
          ))}
        </div>
      </section>

      <section className="ops-grid two">
        <div className="ops-panel">
          <div className="panel-heading"><h2>Execution Statistics</h2></div>
          <div className="result-list">
            <div><span>Records processed</span><strong>{fmt(total || dataset?.lines)}</strong></div>
            <div><span>Mapper output</span><strong>{rows.length ? `${fmt(total)} key/value pairs` : 'pending'}</strong></div>
            <div><span>Reducer output</span><strong>{rows.length ? `${rows.length} result rows` : 'pending'}</strong></div>
            <div><span>Current task</span><strong>{currentTask.label}</strong></div>
            <div><span>Current stage</span><strong>{isRunning ? 'Map / shuffle / reduce' : mrState.status || 'idle'}</strong></div>
          </div>
        </div>

        <div className="ops-panel">
          <div className="panel-heading"><h2>HDFS Storage</h2></div>
          {hdfsStatus?.available ? (
            <div className="hdfs-list">{(hdfsStatus.files || []).slice(0, 10).map((file) => <code key={file.path}>{file.path}</code>)}</div>
          ) : (
            <div className="empty-inline">HDFS is offline. Start the Hadoop services before running a distributed batch job.</div>
          )}
        </div>
      </section>

      <section className="ops-panel">
        <div className="panel-heading"><h2>Batch Results: {currentTask.label}</h2><p>Exact counts emitted by the reducer for the selected task.</p></div>
        {rows.length ? (
          <div className="batch-results">
            {rows.slice(0, 30).map((row) => {
              const percentage = total ? (Number(row.count) / total) * 100 : 0
              const color = activeTask === 'status' ? (STATUS_COLORS[String(row.key)[0]] || '#96a3b6') : '#9b8cff'
              return (
                <div className="batch-result-row wide" key={row.key}>
                  <span className="result-key" title={row.key} style={{ color }}>{row.key}</span>
                  <strong>{Number(row.count).toLocaleString()}</strong>
                  <span className="mono dim">{percentage.toFixed(2)}%</span>
                  <div className="status-track"><div style={{ width: `${Math.max(percentage, row.count ? 1 : 0)}%`, background: color }} /></div>
                </div>
              )
            })}
          </div>
        ) : (
          <div className="empty-inline">Store the dataset in HDFS, then run a MapReduce task to populate reducer results.</div>
        )}
      </section>
    </div>
  )
}

