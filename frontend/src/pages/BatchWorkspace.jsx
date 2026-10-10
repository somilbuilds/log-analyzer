import { useState, useEffect, useMemo, useCallback } from 'react'
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area, PieChart, Pie, Cell,
  ComposedChart, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Treemap, Legend
} from 'recharts'
import { usePolling, apiFetch, apiPost, formatBytes, formatNumber, CHART_COLORS } from '../hooks/useApi'

const CHART_THEME = {
  axisStroke: '#333448',
  tickFill: '#6b6d7a',
  gridStroke: '#1e1f2e',
  tooltipBg: '#1c1d28',
  tooltipBorder: '#333448',
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div className="chart-tooltip">
      <div className="chart-tooltip-label">{label}</div>
      {payload.map((p, i) => (
        <div className="chart-tooltip-row" key={i}>
          <span className="chart-tooltip-dot" style={{ background: p.color }} />
          <span>{p.name || p.dataKey}</span>
          <span className="chart-tooltip-value">{typeof p.value === 'number' ? p.value.toLocaleString() : p.value}</span>
        </div>
      ))}
    </div>
  )
}

function ResultChart({ data, chartType, taskId }) {
  if (!data || data.length === 0) {
    return (
      <div className="state-message">
        <div className="state-icon">📊</div>
        <div className="state-title">No Results Available</div>
        <div className="state-desc">Click "Run Analysis" to execute this MapReduce job on HDFS</div>
      </div>
    )
  }

  const top = data.slice(0, 30)
  const commonProps = { margin: { top: 8, right: 16, left: 8, bottom: 4 } }

  switch (chartType) {
    case 'bar':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={top} layout="vertical" {...commonProps} margin={{ ...commonProps.margin, left: 90 }}>
            <CartesianGrid stroke={CHART_THEME.gridStroke} horizontal={false} />
            <XAxis type="number" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
            <YAxis type="category" dataKey="key" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} width={90} />
            <Tooltip content={<CustomTooltip />} />
            <Bar dataKey="count" fill={CHART_COLORS[0]} radius={[0, 3, 3, 0]} maxBarSize={18} />
          </BarChart>
        </ResponsiveContainer>
      )

    case 'line':
    case 'area': {
      const ChartComp = chartType === 'area' ? AreaChart : LineChart
      return (
        <ResponsiveContainer width="100%" height="100%">
          <ChartComp data={top} {...commonProps}>
            <CartesianGrid stroke={CHART_THEME.gridStroke} />
            <XAxis dataKey="key" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
            <YAxis tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
            <Tooltip content={<CustomTooltip />} />
            {chartType === 'area' ? (
              <Area type="monotone" dataKey="count" stroke={CHART_COLORS[1]} fill={CHART_COLORS[1]} fillOpacity={0.15} strokeWidth={2} dot={false} />
            ) : (
              <Line type="monotone" dataKey="count" stroke={CHART_COLORS[1]} strokeWidth={2} dot={false} />
            )}
          </ChartComp>
        </ResponsiveContainer>
      )
    }

    case 'pie':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie data={top.slice(0, 10)} dataKey="count" nameKey="key" cx="50%" cy="50%" innerRadius="35%" outerRadius="70%"
                 label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                 labelLine={{ stroke: '#4a4b5e' }}
                 style={{ fontSize: 10, fill: '#9496a3' }}>
              {top.slice(0, 10).map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
            </Pie>
            <Tooltip content={<CustomTooltip />} />
          </PieChart>
        </ResponsiveContainer>
      )

    case 'histogram':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={top} {...commonProps}>
            <CartesianGrid stroke={CHART_THEME.gridStroke} />
            <XAxis dataKey="key" tick={{ fill: CHART_THEME.tickFill, fontSize: 9 }} angle={-30} textAnchor="end" height={50} />
            <YAxis tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
            <Tooltip content={<CustomTooltip />} />
            <Bar dataKey="count" fill={CHART_COLORS[2]} radius={[3, 3, 0, 0]} maxBarSize={32} />
          </BarChart>
        </ResponsiveContainer>
      )

    case 'heatmap':
      return <HeatmapChart data={data} />

    case 'stacked':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={top} {...commonProps}>
            <CartesianGrid stroke={CHART_THEME.gridStroke} />
            <XAxis dataKey="key" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
            <YAxis tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
            <Tooltip content={<CustomTooltip />} />
            <Bar dataKey="count" fill={CHART_COLORS[0]} radius={[3, 3, 0, 0]} maxBarSize={24} />
          </BarChart>
        </ResponsiveContainer>
      )

    case 'pareto':
      return <ParetoChart data={data} />

    case 'treemap':
      return (
        <ResponsiveContainer width="100%" height="100%">
          <Treemap data={top.slice(0, 20).map(d => ({ name: d.key, size: d.count }))}
                   dataKey="size" nameKey="name" ratio={4 / 3} stroke="#222333"
                   content={<TreemapContent />} />
        </ResponsiveContainer>
      )

    case 'table':
    default:
      return null
  }
}

function TreemapContent({ x, y, width, height, name, size }) {
  if (width < 30 || height < 20) return null
  return (
    <g>
      <rect x={x} y={y} width={width} height={height} fill={CHART_COLORS[0]} fillOpacity={0.25} stroke="#222333" />
      <text x={x + 4} y={y + 14} fill="#e4e5ea" fontSize={10} fontFamily="Inter">{width > 60 ? name?.slice(0, 12) : ''}</text>
      <text x={x + 4} y={y + 26} fill="#9496a3" fontSize={9} fontFamily="Inter">{width > 50 ? size?.toLocaleString() : ''}</text>
    </g>
  )
}

function HeatmapChart({ data }) {
  const grid = {}
  const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
  data.forEach(d => {
    const parts = d.key.split(' ')
    if (parts.length === 2) {
      const day = parts[0]
      const hour = parts[1]
      if (!grid[day]) grid[day] = {}
      grid[day][hour] = d.count
    }
  })
  const maxVal = Math.max(...data.map(d => d.count), 1)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 2, padding: 'var(--sp-2)', height: '100%', justifyContent: 'center' }}>
      <div style={{ display: 'flex', gap: 2 }}>
        <div style={{ width: 36 }} />
        {Array.from({ length: 24 }, (_, i) => (
          <div key={i} style={{ flex: 1, textAlign: 'center', fontSize: 9, color: 'var(--text-muted)' }}>
            {i.toString().padStart(2, '0')}
          </div>
        ))}
      </div>
      {days.map(day => (
        <div key={day} style={{ display: 'flex', gap: 2, alignItems: 'center' }}>
          <div style={{ width: 36, fontSize: 10, color: 'var(--text-muted)', textAlign: 'right', paddingRight: 4 }}>{day}</div>
          {Array.from({ length: 24 }, (_, h) => {
            const val = grid[day]?.[h.toString().padStart(2, '0')] || 0
            const intensity = val / maxVal
            return (
              <div key={h} title={`${day} ${h}:00 — ${val.toLocaleString()}`}
                   style={{
                     flex: 1, height: 18, borderRadius: 2,
                     background: `rgba(45, 212, 191, ${0.08 + intensity * 0.82})`,
                   }} />
            )
          })}
        </div>
      ))}
    </div>
  )
}

function ParetoChart({ data }) {
  const sorted = [...data].sort((a, b) => b.count - a.count)
  const total = sorted.reduce((s, d) => s + d.count, 0)
  let cumulative = 0
  const paretoData = sorted.slice(0, 30).map((d, i) => {
    cumulative += d.count
    return { ...d, cumPct: +(cumulative / total * 100).toFixed(1), rank: i + 1 }
  })
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={paretoData} margin={{ top: 8, right: 40, left: 8, bottom: 4 }}>
        <CartesianGrid stroke={CHART_THEME.gridStroke} />
        <XAxis dataKey="rank" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
        <YAxis yAxisId="left" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
        <YAxis yAxisId="right" orientation="right" domain={[0, 100]} tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} unit="%" />
        <Tooltip content={<CustomTooltip />} />
        <Bar yAxisId="left" dataKey="count" fill={CHART_COLORS[0]} radius={[3, 3, 0, 0]} maxBarSize={20} />
        <Line yAxisId="right" type="monotone" dataKey="cumPct" stroke={CHART_COLORS[3]} strokeWidth={2} dot={false} />
      </ComposedChart>
    </ResponsiveContainer>
  )
}

function ResultTable({ data, topN }) {
  const rows = data.slice(0, topN)
  const total = data.reduce((s, d) => s + d.count, 0)
  return (
    <div style={{ overflow: 'auto', height: '100%' }}>
      <table className="data-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Key</th>
            <th className="numeric">Count</th>
            <th className="numeric">Share</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i}>
              <td className="numeric">{i + 1}</td>
              <td style={{ maxWidth: 240, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontFamily: 'var(--font-mono)' }}>{row.key}</td>
              <td className="numeric">{row.count.toLocaleString()}</td>
              <td className="numeric">{total ? (row.count / total * 100).toFixed(1) + '%' : '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// Generate text-based analytical findings from batch task output
function generateFindings(results, taskConfig) {
  if (!results || !results.rows || results.rows.length === 0) return null

  const rows = results.rows
  const total = results.total || rows.reduce((s, r) => s + r.count, 0)
  const topKey = rows[0]
  const topPct = total > 0 ? ((topKey.count / total) * 100).toFixed(1) : 0

  const insights = []

  // Top item dominance
  insights.push(`Dominant Key: '${topKey.key}' accounts for ${topKey.count.toLocaleString()} requests (${topPct}% of total volume).`)

  // Concentration (Top 5 share)
  if (rows.length >= 5) {
    const top5Total = rows.slice(0, 5).reduce((s, r) => s + r.count, 0)
    const top5Pct = total > 0 ? ((top5Total / total) * 100).toFixed(1) : 0
    insights.push(`Concentration Ratio: The top 5 keys represent ${top5Pct}% of total analyzed traffic (${top5Total.toLocaleString()} requests).`)
  }

  // Distribution spread
  insights.push(`Distribution Spread: Unique distinct categories processed = ${results.total_keys || rows.length}.`)

  // Task-specific tailored insights
  if (taskConfig?.id === 'status') {
    const ok200 = rows.find(r => r.key === '200')?.count || 0
    const err404 = rows.find(r => r.key === '404')?.count || 0
    const err500 = rows.find(r => r.key === '500')?.count || 0
    insights.push(`HTTP Health: 200 OK accounts for ${((ok200/total)*100).toFixed(1)}%, while 404 Client Errors are ${((err404/total)*100).toFixed(1)}%.`)
  } else if (taskConfig?.id === 'hourly') {
    const peakHour = topKey.key
    insights.push(`Peak Traffic Window: Hour ${peakHour}:00 shows maximum request density.`)
  } else if (taskConfig?.id === 'methods') {
    insights.push(`Protocol Distribution: GET requests comprise the primary access method for ClarkNet assets.`)
  }

  return insights
}

export default function BatchWorkspace() {
  const { data: catalog } = usePolling('/mapreduce/catalog', 60000)
  const { data: mrStatus, refetch: refetchStatus } = usePolling('/mapreduce/status', 3000)
  const { data: hdfsReport } = usePolling('/hdfs/report', 15000)
  const { data: fsck } = usePolling('/hdfs/fsck', 15000)

  const [selectedTask, setSelectedTask] = useState('status')
  const [results, setResults] = useState(null)
  const [resultsLoading, setResultsLoading] = useState(false)
  const [viewMode, setViewMode] = useState('chart') // chart | table | compare | findings
  const [topN, setTopN] = useState(25)
  const [search, setSearch] = useState('')
  const [showHdfs, setShowHdfs] = useState(false)

  // Comparison state
  const [compareTaskId, setCompareTaskId] = useState('hourly')
  const [compareResults, setCompareResults] = useState(null)
  const [compareLoading, setCompareLoading] = useState(false)

  const tasks = catalog?.tasks || []
  const taskConfig = tasks.find(t => t.id === selectedTask)

  // Group by category
  const categories = useMemo(() => {
    const map = {}
    const filtered = tasks.filter(t =>
      !search || t.name.toLowerCase().includes(search.toLowerCase()) ||
      t.description.toLowerCase().includes(search.toLowerCase())
    )
    filtered.forEach(t => {
      if (!map[t.category]) map[t.category] = []
      map[t.category].push(t)
    })
    return map
  }, [tasks, search])

  // Load primary results
  const loadResults = useCallback(async (taskId) => {
    setResultsLoading(true)
    try {
      const r = await apiFetch(`/mapreduce/results?task=${taskId}`)
      setResults(r)
    } catch {
      setResults(null)
    }
    setResultsLoading(false)
  }, [])

  // Load comparison results
  const loadCompareResults = useCallback(async (taskId) => {
    setCompareLoading(true)
    try {
      const r = await apiFetch(`/mapreduce/results?task=${taskId}`)
      setCompareResults(r)
    } catch {
      setCompareResults(null)
    }
    setCompareLoading(false)
  }, [])

  useEffect(() => {
    loadResults(selectedTask)
  }, [selectedTask, loadResults])

  useEffect(() => {
    if (viewMode === 'compare') {
      loadCompareResults(compareTaskId)
    }
  }, [viewMode, compareTaskId, loadCompareResults])

  const handleRun = async () => {
    await apiPost('/mapreduce/run', { task: selectedTask })
    refetchStatus()
  }

  const handleExport = async (format) => {
    window.open(`/api/export/batch/${selectedTask}?format=${format}`, '_blank')
  }

  // Save reproducible experiment config
  const handleSaveExperiment = () => {
    const expConfig = {
      task_id: selectedTask,
      task_name: taskConfig?.name,
      chart_type: taskConfig?.chart,
      topN,
      timestamp: new Date().toISOString(),
      total_records: results?.total,
      unique_keys: results?.total_keys
    }
    const blob = new Blob([JSON.stringify(expConfig, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `experiment_${selectedTask}_${Date.now()}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  const isRunning = mrStatus?.running
  const jobStatus = mrStatus?.status || 'idle'

  const displayData = useMemo(() => {
    if (!results?.rows) return []
    if (selectedTask === 'unusual_paths' || selectedTask === 'rare_status') {
      return [...results.rows].reverse().slice(0, topN)
    }
    return results.rows.slice(0, topN)
  }, [results, selectedTask, topN])

  const findings = useMemo(() => generateFindings(results, taskConfig), [results, taskConfig])
  const summary = hdfsReport?.summary || {}

  return (
    <div className="workspace">
      {/* Metric Strip */}
      <div className="metric-strip">
        <div className="metric-card">
          <div className="metric-label">Processed Output</div>
          <div className="metric-value teal">{formatNumber(results?.total || 0)}</div>
          <div className="metric-sub">total counts aggregated</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Distinct Keys</div>
          <div className="metric-value cyan">{formatNumber(results?.total_keys || 0)}</div>
          <div className="metric-sub">unique categories</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS Storage</div>
          <div className="metric-value blue">{formatBytes(summary.configured_capacity)}</div>
          <div className="metric-sub">Used: {summary.dfs_used_pct ? summary.dfs_used_pct + '%' : '—'}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Live DataNodes</div>
          <div className="metric-value">{summary.live_datanodes ?? '—'}</div>
          <div className="metric-sub">Dead: {summary.dead_datanodes ?? 0}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">MapReduce Job</div>
          <div className={`metric-value ${isRunning ? 'cyan' : jobStatus === 'completed' ? 'green' : ''}`}>
            {isRunning ? 'Running' : jobStatus}
          </div>
          <div className="metric-sub">{mrStatus?.task || 'idle'}</div>
        </div>
      </div>

      {/* Main body: Task Catalog + Main Canvas + Inspector */}
      <div className="workspace-body">
        {/* Task Catalog Sidebar */}
        <div className="panel" style={{ width: 220, flexShrink: 0 }}>
          <div className="panel-header">
            <span className="panel-title">Task Catalog</span>
            <span className="tag tag-blue">{tasks.length}</span>
          </div>
          <div style={{ padding: 'var(--sp-2)' }}>
            <input className="search-input" placeholder="Search tasks…" value={search}
                   onChange={e => setSearch(e.target.value)} />
          </div>
          <div className="panel-body-flush task-list">
            {Object.entries(categories).map(([cat, items]) => (
              <div key={cat}>
                <div className="task-category">{cat}</div>
                {items.map(t => (
                  <div key={t.id}
                       className={`task-item ${selectedTask === t.id ? 'active' : ''}`}
                       onClick={() => setSelectedTask(t.id)}>
                    <div className="task-item-name">{t.name}</div>
                    <div className="task-item-desc">{t.description}</div>
                  </div>
                ))}
              </div>
            ))}
          </div>
        </div>

        {/* Main Workspace Canvas */}
        <div className="panel flex-1">
          <div className="panel-header">
            <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)' }}>
              <span className="panel-title">{taskConfig?.name || selectedTask}</span>
              <span className="tag tag-blue">{taskConfig?.chart || 'table'}</span>
            </div>
            <div className="panel-actions">
              <select className="select" value={topN} onChange={e => setTopN(+e.target.value)}>
                <option value={10}>Top 10</option>
                <option value={25}>Top 25</option>
                <option value={50}>Top 50</option>
                <option value={100}>Top 100</option>
              </select>
              <button className={`btn btn-sm ${viewMode === 'chart' ? 'btn-primary' : ''}`} onClick={() => setViewMode('chart')}>Chart</button>
              <button className={`btn btn-sm ${viewMode === 'table' ? 'btn-primary' : ''}`} onClick={() => setViewMode('table')}>Table</button>
              <button className={`btn btn-sm ${viewMode === 'findings' ? 'btn-primary' : ''}`} onClick={() => setViewMode('findings')}>Findings</button>
              <button className={`btn btn-sm ${viewMode === 'compare' ? 'btn-primary' : ''}`} onClick={() => setViewMode('compare')}>Compare</button>
              <button className="btn btn-sm" onClick={handleSaveExperiment} title="Save Reproducible Config">💾 Config</button>
              <button className="btn btn-sm" onClick={() => handleExport('csv')}>CSV</button>
              <button className="btn btn-sm" onClick={() => handleExport('json')}>JSON</button>
              <button className={`btn ${isRunning ? '' : 'btn-primary'}`} onClick={handleRun} disabled={isRunning}>
                {isRunning ? '⏳ Running…' : '▶ Run Analysis'}
              </button>
            </div>
          </div>

          <div className="panel-body" style={{ padding: (viewMode === 'table' || viewMode === 'findings') ? 0 : 'var(--sp-3)' }}>
            {resultsLoading ? (
              <div className="state-message">
                <div className="spinner" />
                <div className="state-desc">Loading batch analytical results…</div>
              </div>
            ) : viewMode === 'chart' ? (
              <div className="chart-container">
                <ResultChart data={displayData} chartType={taskConfig?.chart || 'bar'} taskId={selectedTask} />
              </div>
            ) : viewMode === 'table' ? (
              <ResultTable data={displayData} topN={topN} />
            ) : viewMode === 'findings' ? (
              <div style={{ padding: 'var(--sp-4)', display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
                <div style={{ fontSize: 'var(--text-md)', fontWeight: 600, color: 'var(--accent-teal)' }}>
                  🔍 Analytical Findings & Automated Insights
                </div>
                <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)' }}>
                  Automated statistical breakdown derived from Hadoop MapReduce execution of <strong>{taskConfig?.name}</strong>:
                </div>
                {findings ? (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
                    {findings.map((f, i) => (
                      <div key={i} style={{
                        padding: 'var(--sp-3)',
                        background: 'var(--bg-elevated)',
                        borderLeft: '3px solid var(--accent-teal)',
                        borderRadius: '0 var(--radius-sm) var(--radius-sm) 0',
                        fontSize: 'var(--text-xs)',
                        color: 'var(--text-primary)',
                        lineHeight: 1.6
                      }}>
                        {f}
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="state-message">
                    <div className="state-desc">No findings available. Execute MapReduce analysis first.</div>
                  </div>
                )}
              </div>
            ) : viewMode === 'compare' ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)', height: '100%' }}>
                {/* Secondary Task Selector */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)', padding: 'var(--sp-1) var(--sp-2)', background: 'var(--bg-elevated)', borderRadius: 'var(--radius-sm)' }}>
                  <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-muted)' }}>Compare with:</span>
                  <select className="select" value={compareTaskId} onChange={e => setCompareTaskId(e.target.value)}>
                    {tasks.map(t => (
                      <option key={t.id} value={t.id}>{t.name} ({t.category})</option>
                    ))}
                  </select>
                </div>

                {/* Side by side chart view */}
                <div style={{ flex: 1, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 'var(--sp-2)', minHeight: 0 }}>
                  <div className="panel" style={{ background: 'var(--bg-subtle)' }}>
                    <div className="panel-header" style={{ height: 28, padding: '0 var(--sp-2)' }}>
                      <span className="panel-title" style={{ fontSize: 10 }}>{taskConfig?.name} (Primary)</span>
                    </div>
                    <div className="panel-body-flush" style={{ padding: 'var(--sp-1)' }}>
                      <ResultChart data={displayData} chartType={taskConfig?.chart || 'bar'} taskId={selectedTask} />
                    </div>
                  </div>

                  <div className="panel" style={{ background: 'var(--bg-subtle)' }}>
                    <div className="panel-header" style={{ height: 28, padding: '0 var(--sp-2)' }}>
                      <span className="panel-title" style={{ fontSize: 10 }}>
                        {tasks.find(t => t.id === compareTaskId)?.name || compareTaskId} (Comparison)
                      </span>
                    </div>
                    <div className="panel-body-flush" style={{ padding: 'var(--sp-1)' }}>
                      {compareLoading ? (
                        <div className="state-message"><div className="spinner" /></div>
                      ) : (
                        <ResultChart data={compareResults?.rows?.slice(0, topN) || []} chartType={tasks.find(t => t.id === compareTaskId)?.chart || 'bar'} taskId={compareTaskId} />
                      )}
                    </div>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        </div>

        {/* Right Inspector */}
        <div className="panel" style={{ width: 200, flexShrink: 0 }}>
          <div className="panel-header">
            <span className="panel-title">Inspector</span>
            <button className="btn btn-sm" onClick={() => setShowHdfs(!showHdfs)}>
              {showHdfs ? 'Job' : 'HDFS'}
            </button>
          </div>
          <div className="panel-body" style={{ fontSize: 'var(--text-xs)' }}>
            {showHdfs ? (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
                <InfoRow label="Capacity" value={formatBytes(summary.configured_capacity)} />
                <InfoRow label="Present" value={formatBytes(summary.present_capacity)} />
                <InfoRow label="DFS Used" value={formatBytes(summary.dfs_used)} />
                <InfoRow label="DFS Remaining" value={formatBytes(summary.dfs_remaining)} />
                <InfoRow label="Used %" value={summary.dfs_used_pct ? summary.dfs_used_pct + '%' : '—'} />
                <InfoRow label="Live Nodes" value={summary.live_datanodes ?? '—'} />
                <InfoRow label="Dead Nodes" value={summary.dead_datanodes ?? 0} />
                <InfoRow label="Total Blocks" value={fsck?.total_blocks ?? '—'} />
                <InfoRow label="Replication" value={fsck?.replication_factor ? fsck.replication_factor + '×' : '—'} />
                <InfoRow label="Dataset Size" value={formatBytes(fsck?.total_size)} />
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
                <InfoRow label="Task" value={mrStatus?.task || '—'} />
                <InfoRow label="Status" value={jobStatus} class={isRunning ? 'running' : jobStatus === 'completed' ? 'online' : 'idle'} />
                {results?.available && (
                  <>
                    <InfoRow label="Output Keys" value={results.total_keys?.toLocaleString()} />
                    <InfoRow label="Total Count" value={results.total?.toLocaleString()} />
                    <InfoRow label="Chart Type" value={results.chart_type} />
                    <InfoRow label="Category" value={results.category} />
                  </>
                )}
                {isRunning && (
                  <div style={{ marginTop: 'var(--sp-2)' }}>
                    <div className="progress-bar">
                      <div className="progress-fill pulse" style={{ width: '60%' }} />
                    </div>
                    <div style={{ color: 'var(--text-muted)', marginTop: 'var(--sp-1)', textAlign: 'center' }}>
                      MapReduce executing…
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function InfoRow({ label, value, class: cls }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', lineHeight: 1.7 }}>
      <span style={{ color: 'var(--text-muted)' }}>{label}</span>
      <span className={cls ? `status-indicator ${cls}` : ''} style={{ fontVariantNumeric: 'tabular-nums' }}>
        {value}
      </span>
    </div>
  )
}
