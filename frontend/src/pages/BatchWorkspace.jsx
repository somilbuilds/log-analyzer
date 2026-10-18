import { useState, useEffect, useMemo, useCallback, useRef } from 'react'
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area, PieChart, Pie, Cell,
  ComposedChart, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Treemap, Legend
} from 'recharts'
import { usePolling, apiFetch, apiPost, formatBytes, formatNumber, CHART_COLORS, API_BASE } from '../hooks/useApi'

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

function MapReduceProgressMonitor({ mrStatus, taskConfig, taskId }) {
  const mapPct = Math.min(100, Math.max(0, mrStatus?.map_progress ?? 0))
  const reducePct = Math.min(100, Math.max(0, mrStatus?.reduce_progress ?? 0))
  const phase = mrStatus?.phase || 'running'

  const phaseLabel = {
    validating_input: 'Checking & Validating HDFS Input Dataset',
    staging: 'Preparing Isolated HDFS Staging Directory',
    submitting: 'Submitting Hadoop Streaming Job to YARN',
    running: 'Application Accepted — Allocating Containers',
    mapping: `Map Phase in Progress (${mapPct}%)`,
    reducing: `Reduce Phase in Progress (${reducePct}%)`,
    publishing: 'Validating & Publishing Staged Results to Final Path',
    completed: 'MapReduce Job Succeeded',
  }[phase] || (mapPct < 100 ? `Map Phase (${mapPct}%)` : reducePct < 100 ? `Reduce Phase (${reducePct}%)` : 'Processing…')

  return (
    <div className="state-message" style={{ padding: 'var(--sp-6)', maxWidth: 580, margin: '0 auto' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)', marginBottom: 'var(--sp-1)' }}>
        <div className="spinner" style={{ width: 28, height: 28, borderWidth: 3 }} />
        <div className="state-title" style={{ color: 'var(--accent-teal)', margin: 0, fontSize: 'var(--text-lg)' }}>
          MapReduce Execution In Progress
        </div>
      </div>

      <div style={{ fontSize: 13, color: 'var(--text-primary)', fontWeight: 600, margin: 'var(--sp-2) 0 4px 0' }}>
        {phaseLabel}
      </div>

      <div className="state-desc" style={{ maxWidth: 480, margin: '0 0 var(--sp-4) 0', fontSize: 12 }}>
        Processing <strong>3,328,632 records</strong> in HDFS (<code>/user/data/raw_logs/access_log</code>) for <strong>{taskConfig?.name || taskId}</strong>.
      </div>

      {/* Live Map & Reduce Progress Bars */}
      <div style={{ width: '100%', background: 'var(--bg-card)', border: '1px solid var(--border-color)', borderRadius: 'var(--radius-md)', padding: 'var(--sp-4)', textAlign: 'left' }}>
        {/* Map Progress (0 to 100%) */}
        <div style={{ marginBottom: 'var(--sp-3)' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6, fontSize: 12 }}>
            <span style={{ fontWeight: 600, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span className={`status-indicator ${mapPct >= 100 ? 'online' : mapPct > 0 ? 'running' : 'idle'}`} />
              Map Phase
            </span>
            <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color: mapPct >= 100 ? 'var(--color-success)' : 'var(--accent-teal)' }}>
              {mapPct}%
            </span>
          </div>
          <div className="progress-bar" style={{ height: 8, background: 'var(--bg-input)' }}>
            <div
              className={`progress-fill ${mapPct < 100 && mapPct > 0 ? 'pulse' : ''}`}
              style={{
                width: `${mapPct}%`,
                background: mapPct >= 100 ? 'var(--color-success)' : 'var(--accent-teal)',
                transition: 'width 0.4s ease-in-out',
              }}
            />
          </div>
        </div>

        {/* Reduce Progress (0 to 100%) */}
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6, fontSize: 12 }}>
            <span style={{ fontWeight: 600, color: 'var(--text-primary)', display: 'flex', alignItems: 'center', gap: 6 }}>
              <span className={`status-indicator ${reducePct >= 100 ? 'online' : reducePct > 0 ? 'running' : 'idle'}`} />
              Reduce Phase
            </span>
            <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, color: reducePct >= 100 ? 'var(--color-success)' : 'var(--accent-blue)' }}>
              {reducePct}%
            </span>
          </div>
          <div className="progress-bar" style={{ height: 8, background: 'var(--bg-input)' }}>
            <div
              className={`progress-fill ${reducePct < 100 && reducePct > 0 ? 'pulse' : ''}`}
              style={{
                width: `${reducePct}%`,
                background: reducePct >= 100 ? 'var(--color-success)' : 'var(--accent-blue)',
                transition: 'width 0.4s ease-in-out',
              }}
            />
          </div>
        </div>

        {/* Job metadata footer */}
        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 'var(--sp-3)', paddingTop: 'var(--sp-2)', borderTop: '1px solid var(--border-color)', fontSize: 11, color: 'var(--text-muted)' }}>
          <span>Mapper: <code>{taskConfig?.mapper || taskId + '_mapper.py'}</code></span>
          <span>Reducer: <code>{taskConfig?.reducer || 'reducer.py'}</code></span>
          {mrStatus?.application_id && (
            <span style={{ fontFamily: 'var(--font-mono)' }}>{mrStatus.application_id}</span>
          )}
        </div>
      </div>
    </div>
  )
}

function ResultChart({ data, chartType, taskId, taskConfig, isRunning, mrStatus }) {
  if (isRunning) {
    return <MapReduceProgressMonitor mrStatus={mrStatus} taskConfig={taskConfig} taskId={taskId} />
  }

  if (!data || data.length === 0) {
    return (
      <div className="state-message" style={{ padding: 'var(--sp-6)' }}>
        <div className="state-title">{taskConfig?.name || taskId}</div>
        <div className="state-desc" style={{ maxWidth: 460, margin: 'var(--sp-2) 0' }}>
          No results available. Run the analysis to generate output data.
        </div>
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
      return <StackedChart data={data} />

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
      // Table-type tasks should default to table view, but if chart view is forced
      // show a bar chart as fallback
      return (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={top} layout="vertical" {...commonProps} margin={{ ...commonProps.margin, left: 90 }}>
            <CartesianGrid stroke={CHART_THEME.gridStroke} horizontal={false} />
            <XAxis type="number" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
            <YAxis type="category" dataKey="key" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} width={90} />
            <Tooltip content={<CustomTooltip />} />
            <Bar dataKey="count" fill={CHART_COLORS[4]} radius={[0, 3, 3, 0]} maxBarSize={18} />
          </BarChart>
        </ResponsiveContainer>
      )

    default:
      return (
        <div className="state-message" style={{ padding: 'var(--sp-4)' }}>
          <div className="state-desc">Unknown chart type: {chartType}. Switching to table view.</div>
        </div>
      )
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
    // Support both "Mon 00" (space-separated) and "Mon:00" (colon-separated)
    const parts = d.key.includes(':') ? d.key.split(':') : d.key.split(' ')
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

/** Proper stacked bar chart for method_status data */
function StackedChart({ data }) {
  // data rows have {key: "GET:200", method: "GET", status: "200", count: 123}
  // Group by method, stack by status
  const methodMap = {}
  const allStatuses = new Set()

  data.forEach(d => {
    const method = d.method || (d.key.includes(':') ? d.key.split(':')[0] : d.key)
    const status = d.status || (d.key.includes(':') ? d.key.split(':')[1] : 'unknown')
    allStatuses.add(status)
    if (!methodMap[method]) methodMap[method] = { method }
    methodMap[method][status] = (methodMap[method][status] || 0) + d.count
  })

  const chartData = Object.values(methodMap)
  const statuses = [...allStatuses].sort()

  // Color mapping for status code groups
  const statusColor = (s) => {
    if (s.startsWith('2')) return '#22d3ee'
    if (s.startsWith('3')) return '#2dd4bf'
    if (s.startsWith('4')) return '#f59e0b'
    if (s.startsWith('5')) return '#ef4444'
    return CHART_COLORS[0]
  }

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={chartData} margin={{ top: 8, right: 16, left: 8, bottom: 4 }}>
        <CartesianGrid stroke={CHART_THEME.gridStroke} />
        <XAxis dataKey="method" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
        <YAxis tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
        <Tooltip content={<CustomTooltip />} />
        <Legend wrapperStyle={{ fontSize: 10 }} />
        {statuses.map(s => (
          <Bar key={s} dataKey={s} stackId="status" fill={statusColor(s)} maxBarSize={32} />
        ))}
      </BarChart>
    </ResponsiveContainer>
  )
}

/** Error rate chart — shows total, error count, and error percentage as dual-axis */
function ErrorRateChart({ data }) {
  return (
    <ResponsiveContainer width="100%" height="100%">
      <ComposedChart data={data} margin={{ top: 8, right: 40, left: 8, bottom: 4 }}>
        <CartesianGrid stroke={CHART_THEME.gridStroke} />
        <XAxis dataKey="key" tick={{ fill: CHART_THEME.tickFill, fontSize: 9 }} angle={-30} textAnchor="end" height={50} />
        <YAxis yAxisId="left" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} />
        <YAxis yAxisId="right" orientation="right" tick={{ fill: CHART_THEME.tickFill, fontSize: 10 }} unit="%" />
        <Tooltip content={<CustomTooltip />} />
        <Legend wrapperStyle={{ fontSize: 10 }} />
        <Bar yAxisId="left" dataKey="total" name="Total Requests" fill={CHART_COLORS[0]} opacity={0.4} maxBarSize={20} />
        <Bar yAxisId="left" dataKey="error" name="Error Requests" fill={CHART_COLORS[3]} maxBarSize={20} />
        <Line yAxisId="right" type="monotone" dataKey="error_pct" name="Error %" stroke="#ef4444" strokeWidth={2} dot={false} />
      </ComposedChart>
    </ResponsiveContainer>
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

function ResultTable({ data, topN, taskId }) {
  const rows = data.slice(0, topN)
  const total = data.reduce((s, d) => s + d.count, 0)
  const hasMultiCol = rows.some(r => r.method || r.total || r.error_pct !== undefined)

  if (hasMultiCol && rows[0]?.method) {
    // method_status table
    return (
      <div style={{ overflow: 'auto', height: '100%' }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Method</th>
              <th>Status</th>
              <th className="numeric">Count</th>
              <th className="numeric">Share</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i}>
                <td className="numeric">{i + 1}</td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>{row.method || row.key}</td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>{row.status || '—'}</td>
                <td className="numeric">{row.count.toLocaleString()}</td>
                <td className="numeric">{total ? (row.count / total * 100).toFixed(1) + '%' : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

  if (hasMultiCol && rows[0]?.error_pct !== undefined) {
    // error_rate_day table
    return (
      <div style={{ overflow: 'auto', height: '100%' }}>
        <table className="data-table">
          <thead>
            <tr>
              <th>#</th>
              <th>Day</th>
              <th className="numeric">Total</th>
              <th className="numeric">Errors</th>
              <th className="numeric">Error %</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => (
              <tr key={i}>
                <td className="numeric">{i + 1}</td>
                <td style={{ fontFamily: 'var(--font-mono)' }}>{row.key}</td>
                <td className="numeric">{(row.total || 0).toLocaleString()}</td>
                <td className="numeric">{(row.error || row.count || 0).toLocaleString()}</td>
                <td className="numeric">{row.error_pct !== undefined ? row.error_pct + '%' : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    )
  }

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
    insights.push(`HTTP Health: 200 OK accounts for ${((ok200/total)*100).toFixed(1)}%, while 404 Client Errors are ${((err404/total)*100).toFixed(1)}%.`)
  } else if (taskConfig?.id === 'hourly') {
    const peakHour = topKey.key
    insights.push(`Peak Traffic Window: Hour ${peakHour}:00 shows maximum request density.`)
  } else if (taskConfig?.id === 'methods') {
    insights.push(`Protocol Distribution: GET requests comprise the primary access method for ClarkNet assets.`)
  } else if (taskConfig?.id === 'error_rate_day') {
    const avgPct = rows.filter(r => r.error_pct !== undefined).reduce((s, r) => s + r.error_pct, 0) / rows.length
    insights.push(`Average Daily Error Rate: ${avgPct.toFixed(2)}% across ${rows.length} days.`)
  }

  return insights
}

export default function BatchWorkspace() {
  const { data: catalog } = usePolling('/mapreduce/catalog', 60000)
  const { data: mrStatus, refetch: refetchStatus } = usePolling('/mapreduce/status', 1500)
  const { data: hdfsReport } = usePolling('/hdfs/report', 15000)
  const { data: fsck } = usePolling('/hdfs/fsck', 15000)

  const [selectedTask, setSelectedTask] = useState('status')
  const [taskResults, setTaskResults] = useState({})
  const [viewMode, setViewMode] = useState('auto') // auto | chart | table | findings | compare
  const [topN, setTopN] = useState(25)
  const [search, setSearch] = useState('')
  const [showHdfs, setShowHdfs] = useState(false)

  // Comparison state
  const [compareTaskId, setCompareTaskId] = useState('hourly')
  const [compareResults, setCompareResults] = useState(null)
  const [compareLoading, setCompareLoading] = useState(false)

  // Per-task loading: which tasks currently have an in-flight fetch
  const [loadingTasks, setLoadingTasks] = useState(new Set())

  // Track which tasks have had results explicitly loaded by user action
  const [fetchedTasks, setFetchedTasks] = useState(new Set())

  // Per-task execution state: { [taskId]: 'idle'|'running'|'completed'|'failed' }
  const [taskRunState, setTaskRunState] = useState({})

  // Monotonic request counter — used to discard stale responses
  const requestIdRef = useRef(0)
  // Map from task ID → latest request ID for that task
  const taskRequestIdRef = useRef({})

  const [runError, setRunError] = useState(null)

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

  // Load primary results for a specific task — only called by explicit user action.
  // Uses a request counter to discard stale responses from out-of-order fetches.
  const loadResults = useCallback(async (taskId) => {
    const reqId = ++requestIdRef.current
    taskRequestIdRef.current[taskId] = reqId

    setLoadingTasks(prev => new Set(prev).add(taskId))
    try {
      const r = await apiFetch(`/mapreduce/results?task=${taskId}`)
      // Only accept result if this is still the latest request for this task
      if (taskRequestIdRef.current[taskId] === reqId) {
        setTaskResults(prev => ({ ...prev, [taskId]: r }))
        setFetchedTasks(prev => new Set(prev).add(taskId))
      }
    } catch (e) {
      if (taskRequestIdRef.current[taskId] === reqId) {
        setTaskResults(prev => ({ ...prev, [taskId]: { available: false, error: e.message, rows: [] } }))
        setFetchedTasks(prev => new Set(prev).add(taskId))
      }
    }
    setLoadingTasks(prev => {
      const next = new Set(prev)
      next.delete(taskId)
      return next
    })
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
    if (viewMode === 'compare') {
      loadCompareResults(compareTaskId)
    }
  }, [viewMode, compareTaskId, loadCompareResults])

  // NO automatic results loading on task selection.
  // The user must explicitly click "Load Results" or "Start MapReduce".

  // Watch mrStatus transitions: when a job stops running, update per-task state
  useEffect(() => {
    if (!mrStatus) return
    const jobTask = mrStatus.task
    if (!jobTask) return

    if (mrStatus.running) {
      // Mark this task as running (in case it was started outside this session)
      setTaskRunState(prev => ({ ...prev, [jobTask]: 'running' }))
      if (mrStatus.status_query_error) {
        setRunError(`ResourceManager status is temporarily unavailable${mrStatus.status_stale ? '; showing the last known YARN state' : ''}. The job is not being marked successful.`)
      }
    } else if (mrStatus.status === 'completed' || mrStatus.success === true) {
      setTaskRunState(prev => {
        if (prev[jobTask] === 'running' || prev[jobTask] === 'unknown') {
          // Job just finished successfully — auto-load results for this task
          loadResults(jobTask)
          return { ...prev, [jobTask]: 'completed' }
        }
        return prev
      })
    } else if (mrStatus.status === 'failed' || mrStatus.status === 'killed' || mrStatus.success === false) {
      setTaskRunState(prev => {
        if (prev[jobTask] === 'running' || prev[jobTask] === 'unknown') {
          setRunError(`MapReduce job ${mrStatus.status} (exit code ${mrStatus.code ?? '?'}). Check the per-job log and logs/mapreduce.log for details.`)
          return { ...prev, [jobTask]: 'failed' }
        }
        return prev
      })
    } else if (mrStatus.status === 'unknown') {
      setTaskRunState(prev => {
        if (prev[jobTask] === 'running') {
          setRunError(`YARN state is unknown${mrStatus.status_query_error ? `: ${mrStatus.status_query_error}` : ''}. Results were not marked successful; check the ResourceManager and per-job log.`)
          return { ...prev, [jobTask]: 'unknown' }
        }
        return prev
      })
    }
  }, [mrStatus?.running, mrStatus?.status, mrStatus?.task, mrStatus?.status_query_error, mrStatus?.status_stale, mrStatus?.code, loadResults])

  const handleSelectTask = useCallback((taskId) => {
    setSelectedTask(taskId)
    setRunError(null)
    // Do NOT auto-load results or auto-execute anything
  }, [])

  const handleRun = async () => {
    setRunError(null)
    setTaskRunState(prev => ({ ...prev, [selectedTask]: 'running' }))
    const res = await apiPost('/mapreduce/run', { task: selectedTask })
    if (res?.status === 'error') {
      setRunError(res.detail || 'MapReduce job already in progress or cluster busy.')
      setTaskRunState(prev => ({ ...prev, [selectedTask]: 'idle' }))
    }
    refetchStatus()
  }

  const handleLoadCached = async () => {
    loadResults(selectedTask)
  }

  const handleExport = async (format) => {
    window.open(`${API_BASE}/export/batch/${selectedTask}?format=${format}`, '_blank')
  }

  // Save reproducible experiment config
  const handleSaveExperiment = () => {
    const r = taskResults[selectedTask]
    const expConfig = {
      task_id: selectedTask,
      task_name: taskConfig?.name,
      chart_type: taskConfig?.chart,
      topN,
      timestamp: new Date().toISOString(),
      total_records: r?.total,
      unique_keys: r?.total_keys
    }
    const blob = new Blob([JSON.stringify(expConfig, null, 2)], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `experiment_${selectedTask}_${Date.now()}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  const taskResult = taskResults[selectedTask]
  const hasFetched = fetchedTasks.has(selectedTask)
  const hasCachedOutput = Boolean(taskResult?.available && taskResult?.rows?.length > 0)
  const hasError = Boolean(hasFetched && taskResult?.error && !taskResult?.available)
  const resultsLoading = loadingTasks.has(selectedTask)

  // Derive job status from explicit per-task execution state, NOT from results.
  const taskExecState = taskRunState[selectedTask] || 'idle'
  const isRunning = taskExecState === 'running'
  const lastJobFailed = taskExecState === 'failed'
  const jobStatus = isRunning ? 'running' : lastJobFailed ? 'failed' : hasCachedOutput ? 'completed' : 'idle'

  // Determine the effective view mode
  const effectiveViewMode = useMemo(() => {
    if (viewMode === 'auto') {
      // Table-type tasks default to table view
      if (taskConfig?.chart === 'table') return 'table'
      return 'chart'
    }
    return viewMode
  }, [viewMode, taskConfig?.chart])

  // Data for display — API now handles sorting properly
  const displayData = useMemo(() => {
    if (!taskResult?.rows) return []
    return taskResult.rows.slice(0, topN)
  }, [taskResult, topN])

  const findings = useMemo(() => generateFindings(taskResult, taskConfig), [taskResult, taskConfig])
  const summary = hdfsReport?.summary || {}

  // Render the appropriate chart component, with special handling for error_rate_day
  const renderChart = (data, config, taskId) => {
    if (config?.id === 'error_rate_day' && data.some(r => r.error_pct !== undefined)) {
      return <ErrorRateChart data={data} />
    }
    return <ResultChart data={data} chartType={config?.chart || 'bar'} taskId={taskId} taskConfig={config} isRunning={isRunning} mrStatus={mrStatus} />
  }

  return (
    <div className="workspace">
      {/* Metric Strip */}
      <div className="metric-strip">
        <div className="metric-card">
          <div className="metric-label">Processed Output</div>
          <div className="metric-value teal">{formatNumber(taskResult?.total || 0)}</div>
          <div className="metric-sub">total counts aggregated</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Distinct Categories</div>
          <div className="metric-value cyan">{formatNumber(taskResult?.total_keys || 0)}</div>
          <div className="metric-sub">unique keys found</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS Dataset</div>
          <div className="metric-value blue">343.4 MB</div>
          <div className="metric-sub">3,328,632 access log records</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Cluster Storage (3×)</div>
          <div className="metric-value violet">1.03 GB</div>
          <div className="metric-sub">3 DataNodes (3 blk × 3 repl = 9)</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">MapReduce Engine</div>
          <div className={`metric-value ${isRunning ? 'cyan' : jobStatus === 'completed' ? 'green' : jobStatus === 'failed' ? '' : ''}`}
               style={jobStatus === 'failed' ? { color: 'var(--color-danger)' } : undefined}>
            {isRunning ? 'Running' : jobStatus === 'completed' ? 'Completed' : jobStatus === 'failed' ? 'Failed' : 'Idle'}
          </div>
          <div className="metric-sub">{isRunning ? `Task: ${selectedTask}` : 'Hadoop 3.2.1 YARN'}</div>
        </div>
      </div>

      {runError && (
        <div style={{
          background: 'rgba(239, 68, 68, 0.15)', border: '1px solid var(--color-danger)',
          borderRadius: 4, padding: '6px 14px', margin: '0 var(--sp-3) var(--sp-2) var(--sp-3)',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 12, color: 'var(--color-danger)'
        }}>
          <span>⚠ {runError}</span>
          <button className="btn btn-sm" onClick={() => setRunError(null)} style={{ padding: '2px 8px' }}>Dismiss</button>
        </div>
      )}

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
                       onClick={() => handleSelectTask(t.id)}>
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
              <button className={`btn btn-sm ${effectiveViewMode === 'chart' && viewMode !== 'auto' ? 'btn-primary' : viewMode === 'auto' && taskConfig?.chart !== 'table' ? 'btn-primary' : ''}`} onClick={() => setViewMode('chart')}>Chart</button>
              <button className={`btn btn-sm ${effectiveViewMode === 'table' && viewMode !== 'auto' ? 'btn-primary' : viewMode === 'auto' && taskConfig?.chart === 'table' ? 'btn-primary' : ''}`} onClick={() => setViewMode('table')}>Table</button>
              <button className={`btn btn-sm ${viewMode === 'findings' ? 'btn-primary' : ''}`} onClick={() => setViewMode('findings')}>Findings</button>
              <button className={`btn btn-sm ${viewMode === 'compare' ? 'btn-primary' : ''}`} onClick={() => setViewMode('compare')}>Compare</button>
              <button className="btn btn-sm" onClick={handleSaveExperiment} title="Save Reproducible Config">💾 Config</button>
              <button className="btn btn-sm" onClick={() => handleExport('csv')}>CSV</button>
              <button className="btn btn-sm" onClick={() => handleExport('json')}>JSON</button>
              <button className={`btn ${isRunning ? '' : 'btn-primary'}`} onClick={handleRun} disabled={isRunning}>
                {isRunning ? '⏳ Running…' : hasCachedOutput ? '🔄 Re-run' : '▶ Start MapReduce'}
              </button>
            </div>
          </div>

          <div className="panel-body" style={{ padding: (!hasCachedOutput || !hasFetched || effectiveViewMode === 'table' || effectiveViewMode === 'findings') ? 0 : 'var(--sp-3)' }}>
            {isRunning ? (
              <MapReduceProgressMonitor mrStatus={mrStatus} taskConfig={taskConfig} taskId={selectedTask} />
            ) : resultsLoading ? (
              <div className="state-message">
                <div className="spinner" />
                <div className="state-desc">Loading batch analytical results…</div>
              </div>
            ) : lastJobFailed ? (
              <div className="state-message" style={{ padding: 'var(--sp-6)' }}>
                <div style={{ fontSize: 24, marginBottom: 'var(--sp-2)', color: 'var(--color-danger)' }}>✕</div>
                <div className="state-title" style={{ color: 'var(--color-danger)' }}>MapReduce Job Failed</div>
                <div className="state-desc" style={{ maxWidth: 460, margin: 'var(--sp-2) 0' }}>
                  The Hadoop Streaming job for <strong>{taskConfig?.name}</strong> did not complete successfully.
                  Check <code>logs/mapreduce.log</code> for detailed error output.
                </div>
                <button className="btn btn-primary" onClick={handleRun} style={{ marginTop: 'var(--sp-2)' }}>
                  ↻ Retry Analysis
                </button>
              </div>
            ) : hasError ? (
              <div className="state-message" style={{ padding: 'var(--sp-6)' }}>
                <div style={{ fontSize: 24, marginBottom: 'var(--sp-2)', color: 'var(--color-danger)' }}>⚠</div>
                <div className="state-title" style={{ color: 'var(--color-danger)' }}>HDFS Read Error</div>
                <div className="state-desc" style={{ maxWidth: 460, margin: 'var(--sp-2) 0', color: 'var(--text-secondary)' }}>
                  {taskResult?.error || 'Failed to read results from HDFS.'}
                </div>
                <div style={{ display: 'flex', gap: 'var(--sp-2)', justifyContent: 'center', marginTop: 'var(--sp-3)' }}>
                  <button className="btn btn-primary" onClick={handleRun} style={{ padding: '8px 24px', fontSize: 13, fontWeight: 600 }}>
                    ▶ Start MapReduce
                  </button>
                  <button className="btn" onClick={handleLoadCached}>↻ Retry Load</button>
                </div>
              </div>
            ) : !hasFetched ? (
              /* Task detail view — shown when task is selected but no results loaded */
              <div className="state-message" style={{ padding: 'var(--sp-6)', maxWidth: 540, margin: '0 auto' }}>
                <div className="state-title" style={{ fontSize: 'var(--text-lg)', color: 'var(--text-primary)' }}>
                  {taskConfig?.name || selectedTask}
                </div>
                <div className="state-desc" style={{ maxWidth: 460, margin: 'var(--sp-2) 0', color: 'var(--text-secondary)' }}>
                  {taskConfig?.description || 'Hadoop MapReduce analysis ready to execute across HDFS.'}
                </div>

                <div style={{
                  background: 'var(--bg-elevated)',
                  border: '1px solid var(--border-default)',
                  borderRadius: 'var(--radius-sm)',
                  padding: 'var(--sp-3)',
                  width: '100%',
                  margin: 'var(--sp-3) 0 var(--sp-4) 0',
                  textAlign: 'left',
                  fontSize: 11
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Category:</span>
                    <span style={{ color: 'var(--accent-cyan)' }}>{taskConfig?.category || '—'}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Input Dataset:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)' }}>/user/data/raw_logs/access_log</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Dataset Volume:</span>
                    <span>343.4 MB · 3,328,632 records</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Cluster Execution:</span>
                    <span>Hadoop Streaming (3 DataNodes, Repl 3×)</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Chart Type:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-teal)' }}>{taskConfig?.chart || 'bar'}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Mapper:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-teal)' }}>{taskConfig?.mapper || selectedTask + '_mapper.py'}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Reducer:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-teal)' }}>{taskConfig?.reducer || 'reducer.py'}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Output Path:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)' }}>{taskConfig?.output || '—'}</span>
                  </div>
                </div>

                <div style={{ display: 'flex', gap: 'var(--sp-2)', justifyContent: 'center' }}>
                  <button className="btn btn-primary" onClick={handleRun}
                          disabled={isRunning}
                          style={{ padding: '10px 28px', fontSize: 14, fontWeight: 600 }}>
                    ▶ Start MapReduce
                  </button>
                  <button className="btn" onClick={handleLoadCached} title="Check if output exists in HDFS from a previous run">
                    Load Existing Results
                  </button>
                </div>
              </div>
            ) : !hasCachedOutput ? (
              <div className="state-message" style={{ padding: 'var(--sp-6)', maxWidth: 540, margin: '0 auto' }}>
                <div className="state-title" style={{ fontSize: 'var(--text-lg)', color: 'var(--text-primary)' }}>
                  {taskConfig?.name || selectedTask}
                </div>
                <div className="state-desc" style={{ maxWidth: 460, margin: 'var(--sp-2) 0', color: 'var(--text-secondary)' }}>
                  No results available in HDFS. Run the analysis to generate output data.
                </div>

                <div style={{
                  background: 'var(--bg-elevated)',
                  border: '1px solid var(--border-default)',
                  borderRadius: 'var(--radius-sm)',
                  padding: 'var(--sp-3)',
                  width: '100%',
                  margin: 'var(--sp-3) 0 var(--sp-4) 0',
                  textAlign: 'left',
                  fontSize: 11
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Input Dataset:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)' }}>/user/data/raw_logs/access_log</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Dataset Volume:</span>
                    <span>343.4 MB · 3,328,632 records</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Cluster Execution:</span>
                    <span>Hadoop Streaming (3 DataNodes, Repl 3×)</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', borderBottom: '1px solid var(--border-default)' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Mapper:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-teal)' }}>{taskConfig?.mapper || selectedTask + '_mapper.py'}</span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0' }}>
                    <span style={{ color: 'var(--text-muted)' }}>Reducer:</span>
                    <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-teal)' }}>{taskConfig?.reducer || 'reducer.py'}</span>
                  </div>
                </div>

                <div style={{ display: 'flex', gap: 'var(--sp-2)', justifyContent: 'center' }}>
                  <button className="btn btn-primary" onClick={handleRun}
                          disabled={isRunning}
                          style={{ padding: '10px 28px', fontSize: 14, fontWeight: 600 }}>
                    ▶ Start MapReduce
                  </button>
                  <button className="btn" onClick={handleLoadCached} title="Re-check HDFS for results">
                    ↻ Retry Load
                  </button>
                </div>
              </div>
            ) : effectiveViewMode === 'chart' ? (
              <div className="chart-container">
                {renderChart(displayData, taskConfig, selectedTask)}
              </div>
            ) : effectiveViewMode === 'table' ? (
              <ResultTable data={displayData} topN={topN} taskId={selectedTask} />
            ) : effectiveViewMode === 'findings' ? (
              <div style={{ padding: 'var(--sp-4)', display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
                <div style={{ fontSize: 'var(--text-md)', fontWeight: 600, color: 'var(--accent-teal)' }}>
                  Analytical Findings & Automated Insights
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
                      {renderChart(displayData, taskConfig, selectedTask)}
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
                        renderChart(
                          compareResults?.rows?.slice(0, topN) || [],
                          tasks.find(t => t.id === compareTaskId),
                          compareTaskId
                        )
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
                <InfoRow label="Task" value={taskConfig?.id || selectedTask} />
                <InfoRow label="Status" value={jobStatus} class={isRunning ? 'running' : jobStatus === 'completed' ? 'online' : jobStatus === 'failed' ? 'error' : 'idle'} />
                <InfoRow label="Mapper" value={taskConfig?.mapper || '—'} />
                <InfoRow label="Reducer" value={taskConfig?.reducer || '—'} />
                {taskResult?.available && (
                  <>
                    <InfoRow label="Output Keys" value={taskResult.total_keys?.toLocaleString()} />
                    <InfoRow label="Total Count" value={taskResult.total?.toLocaleString()} />
                    <InfoRow label="Chart Type" value={taskResult.chart_type} />
                    <InfoRow label="Category" value={taskResult.category} />
                  </>
                )}
                {taskResult?.error && !taskResult?.available && (
                  <div style={{ color: 'var(--color-danger)', fontSize: 10, marginTop: 'var(--sp-1)' }}>
                    {taskResult.error}
                  </div>
                )}
                {isRunning && (
                  <div style={{ marginTop: 'var(--sp-2)', padding: 'var(--sp-2)', background: 'var(--bg-input)', borderRadius: 'var(--radius-sm)' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 3 }}>
                      <span>Map</span>
                      <strong style={{ color: 'var(--accent-teal)' }}>{Math.min(100, Math.max(0, mrStatus?.map_progress ?? 0))}%</strong>
                    </div>
                    <div className="progress-bar" style={{ height: 5, marginBottom: 6 }}>
                      <div className="progress-fill" style={{ width: `${Math.min(100, Math.max(0, mrStatus?.map_progress ?? 0))}%` }} />
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 3 }}>
                      <span>Reduce</span>
                      <strong style={{ color: 'var(--accent-blue)' }}>{Math.min(100, Math.max(0, mrStatus?.reduce_progress ?? 0))}%</strong>
                    </div>
                    <div className="progress-bar" style={{ height: 5, marginBottom: 4 }}>
                      <div className="progress-fill blue" style={{ width: `${Math.min(100, Math.max(0, mrStatus?.reduce_progress ?? 0))}%` }} />
                    </div>
                    <div style={{ color: 'var(--text-muted)', fontSize: 10, textAlign: 'center', marginTop: 4 }}>
                      {mrStatus?.phase === 'publishing' ? 'Publishing to HDFS…' :
                       (mrStatus?.map_progress ?? 0) < 100 ? 'Mapping splits…' :
                       (mrStatus?.reduce_progress ?? 0) < 100 ? 'Reducing partitions…' : 'Finalizing…'}
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
      <span className={cls ? `status-indicator ${cls}` : ''} style={{ fontVariantNumeric: 'tabular-nums', maxWidth: 110, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
        {value}
      </span>
    </div>
  )
}
