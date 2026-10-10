import { useEffect, useState, useMemo } from 'react'
import { Link } from 'react-router-dom'
import { usePolling, formatBytes, formatNumber } from '../hooks/useApi'
import {
  BarChart, Bar, AreaChart, Area, ResponsiveContainer,
  XAxis, YAxis, Tooltip
} from 'recharts'

const CHART_COLORS = ['#3b82f6', '#2dd4bf', '#8b5cf6', '#f59e0b', '#22d3ee']

function MiniTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={{
      background: '#1c1d28', border: '1px solid #333448', borderRadius: 4,
      padding: '4px 8px', fontSize: 10, color: '#e4e5ea'
    }}>
      <div style={{ color: '#6b6d7a', marginBottom: 2 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color }}>
          {p.name}: {typeof p.value === 'number' ? p.value.toLocaleString() : p.value}
        </div>
      ))}
    </div>
  )
}

export default function Overview({ health }) {
  const { data: dataset } = usePolling('/dataset', 10000)
  const { data: hdfs } = usePolling('/hdfs/status', 12000)
  const { data: hdfsReport } = usePolling('/hdfs/report', 15000)
  const { data: fsck } = usePolling('/hdfs/fsck', 15000)
  const { data: status } = usePolling('/status', 4000)
  const { data: jobs } = usePolling('/jobs?limit=8', 10000)
  const { data: metrics } = usePolling('/metrics?limit=20', 6000)

  const records = dataset?.lines || 0
  const fileSize = dataset?.size_bytes || 0
  const hdfsReady = hdfs?.dataset_ready || false
  const dateRange = dataset?.date_range || {}
  const summary = hdfsReport?.summary || {}
  const nodeCount = summary.live_datanodes ?? 0
  const dfsUsed = summary.dfs_used || 0
  const dfsCapacity = summary.configured_capacity || 0
  const dfsUsedPct = summary.dfs_used_pct || 0
  const streamRunning = status?.demo?.replayer && status?.demo?.stream
  const streamState = status?.stream_state || {}
  const windowCount = status?.window_count || 0

  // Mini chart data from recent streaming aggregates
  const miniChartData = useMemo(() => {
    const aggs = metrics?.aggregates || []
    return aggs.slice(-15).map(a => ({
      w: a.window_id,
      req: a.total_requests || 0,
      err: Math.round((a.error_rate || 0) * 100),
    }))
  }, [metrics])

  // Status distribution for mini bar chart
  const statusMini = useMemo(() => {
    const aggs = metrics?.aggregates || []
    return aggs.slice(-12).map(a => {
      const dist = a.status_distribution || {}
      let s2xx = 0, s4xx = 0, s5xx = 0
      Object.entries(dist).forEach(([code, count]) => {
        const c = parseInt(code)
        if (c >= 200 && c < 300) s2xx += count
        else if (c >= 400 && c < 500) s4xx += count
        else if (c >= 500) s5xx += count
      })
      return { w: a.window_id, '2xx': s2xx, '4xx': s4xx, '5xx': s5xx }
    })
  }, [metrics])

  const serviceCount = [health?.hdfs, health?.mongo, health?.ok, health?.dataset].filter(Boolean).length

  return (
    <div className="workspace">
      {/* Metric Strip — 7 cards */}
      <div className="metric-strip">
        <div className="metric-card">
          <div className="metric-label">Dataset Records</div>
          <div className="metric-value teal">{formatNumber(records)}</div>
          <div className="metric-sub">{formatBytes(fileSize)} · CLF</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Date Range</div>
          <div className="metric-value" style={{ fontSize: 'var(--text-sm)' }}>
            {dateRange.first ? dateRange.first.split(':')[0] : '—'}
          </div>
          <div className="metric-sub">
            {dateRange.last ? '→ ' + dateRange.last.split(':')[0] : '1995 Archive'}
          </div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS Dataset</div>
          <div className={`metric-value ${hdfsReady ? 'green' : 'amber'}`}>
            {hdfsReady ? 'Ready' : 'Not Loaded'}
          </div>
          <div className="metric-sub">{nodeCount} DataNode{nodeCount !== 1 ? 's' : ''} live</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS Storage</div>
          <div className="metric-value blue">{formatBytes(dfsUsed)}</div>
          <div className="metric-sub">of {formatBytes(dfsCapacity)} · {dfsUsedPct}%</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS Blocks</div>
          <div className="metric-value">{fsck?.total_blocks ?? '—'}</div>
          <div className="metric-sub">Repl: {fsck?.replication_factor ?? '—'}×</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Stream Pipeline</div>
          <div className={`metric-value ${streamRunning ? 'cyan' : ''}`}>
            {streamRunning ? 'Active' : streamState.status || 'Stopped'}
          </div>
          <div className="metric-sub">Windows: {windowCount}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Services</div>
          <div className={`metric-value ${serviceCount === 4 ? 'green' : serviceCount >= 2 ? 'amber' : 'red'}`}>
            {serviceCount}/4
          </div>
          <div className="metric-sub">HDFS · Mongo · API · Data</div>
        </div>
      </div>

      {/* Main Content Grid */}
      <div style={{ flex: 1, minHeight: 0, display: 'grid', gridTemplateColumns: '1fr 1fr 280px', gap: 'var(--sp-2)', overflow: 'hidden' }}>

        {/* Workspace A Entry */}
        <Link to="/workspace-a" className="workspace-card" style={{ display: 'flex', flexDirection: 'column' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)', marginBottom: 'var(--sp-2)' }}>
            <div className="workspace-card-icon" style={{ color: 'var(--accent-blue)', fontSize: 22, margin: 0 }}>▣</div>
            <div>
              <div className="workspace-card-title" style={{ fontSize: 'var(--text-sm)' }}>Workspace A — HDFS + MapReduce</div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Distributed batch analytics on the complete dataset</div>
            </div>
          </div>
          <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', lineHeight: 1.5, marginBottom: 'var(--sp-2)' }}>
            25 analytical tasks across traffic patterns, HTTP responses, security indicators,
            and resource utilization. Genuine Hadoop Streaming MapReduce execution.
          </div>
          <div style={{ display: 'flex', gap: 'var(--sp-1)', flexWrap: 'wrap', marginTop: 'auto' }}>
            <span className="tag tag-blue">25 Analyses</span>
            <span className="tag tag-teal">MapReduce</span>
            <span className={`tag ${hdfsReady ? 'tag-green' : 'tag-amber'}`}>
              {hdfsReady ? 'HDFS Ready' : 'Setup Required'}
            </span>
          </div>
        </Link>

        {/* Workspace B Entry */}
        <Link to="/workspace-b" className="workspace-card" style={{ display: 'flex', flexDirection: 'column' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)', marginBottom: 'var(--sp-2)' }}>
            <div className="workspace-card-icon" style={{ color: 'var(--accent-violet)', fontSize: 22, margin: 0 }}>◉</div>
            <div>
              <div className="workspace-card-title" style={{ fontSize: 'var(--text-sm)' }}>Workspace B — Spark Streaming Lab</div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>Real-time stream processing with approximate algorithms</div>
            </div>
          </div>
          <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', lineHeight: 1.5, marginBottom: 'var(--sp-2)' }}>
            Replay ClarkNet logs as a controllable stream. Bloom Filter, DGIM, Flajolet-Martin
            approximate algorithms with exact comparison. EWMA forecasting.
          </div>
          <div style={{ display: 'flex', gap: 'var(--sp-1)', flexWrap: 'wrap', marginTop: 'auto' }}>
            <span className="tag tag-violet">Live Analytics</span>
            <span className="tag tag-teal">Stream Mining</span>
            <span className={`tag ${streamRunning ? 'tag-green' : 'tag-blue'}`}>
              {streamRunning ? 'Stream Active' : 'Ready'}
            </span>
          </div>
        </Link>

        {/* Right Column: Infrastructure */}
        <div className="panel" style={{ display: 'flex', flexDirection: 'column' }}>
          <div className="panel-header">
            <span className="panel-title">Infrastructure</span>
            <Link to="/system" className="btn btn-sm">Details</Link>
          </div>
          <div className="panel-body" style={{ padding: 'var(--sp-2)', display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
            <InfraRow label="FastAPI Backend" ok={health?.ok} port="8000" />
            <InfraRow label="HDFS NameNode" ok={health?.hdfs} port="9870" />
            <InfraRow label="MongoDB" ok={health?.mongo} port="27018" />
            <InfraRow label="ClarkNet Dataset" ok={health?.dataset} />
            <InfraRow label="HDFS Dataset" ok={hdfsReady} />
            <InfraRow label="Stream Pipeline" ok={streamRunning} optional />

            {/* HDFS Storage Bar */}
            {dfsCapacity > 0 && (
              <div style={{ marginTop: 'var(--sp-1)', borderTop: '1px solid var(--border-default)', paddingTop: 'var(--sp-2)' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10, color: 'var(--text-muted)', marginBottom: 3 }}>
                  <span>HDFS Capacity</span>
                  <span>{dfsUsedPct}%</span>
                </div>
                <div className="storage-bar">
                  <div className="storage-segment" style={{
                    width: `${Math.max(dfsUsedPct, 1)}%`,
                    background: dfsUsedPct > 80 ? 'var(--color-warning)' : 'var(--accent-blue)'
                  }} />
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 9, color: 'var(--text-muted)', marginTop: 2 }}>
                  <span>{formatBytes(dfsUsed)}</span>
                  <span>{formatBytes(dfsCapacity)}</span>
                </div>
              </div>
            )}

            {/* DataNode Summary */}
            {hdfsReport?.nodes?.length > 0 && (
              <div style={{ borderTop: '1px solid var(--border-default)', paddingTop: 'var(--sp-1)' }}>
                <div style={{ fontSize: 10, color: 'var(--text-muted)', fontWeight: 600, marginBottom: 'var(--sp-1)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>DataNodes</div>
                {hdfsReport.nodes.map((node, i) => (
                  <div key={i} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 10, lineHeight: 1.8 }}>
                    <span style={{ color: 'var(--accent-cyan)', fontFamily: 'var(--font-mono)' }}>{node.hostname || node.address}</span>
                    <span style={{ color: 'var(--text-muted)' }}>{formatBytes(node.used)} / {formatBytes(node.capacity)}</span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Bottom row spans full width: Recent Jobs + Mini Charts */}
        {/* Recent Jobs */}
        <div className="panel" style={{ gridColumn: '1 / 3' }}>
          <div className="panel-header">
            <span className="panel-title">Recent Jobs</span>
            <Link to="/jobs" className="btn btn-sm">View All</Link>
          </div>
          <div className="panel-body-flush" style={{ overflow: 'auto' }}>
            {jobs?.jobs?.length > 0 ? (
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Started</th>
                    <th>Duration</th>
                    <th>Input</th>
                    <th>Output</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.jobs.slice(0, 6).map((job, i) => (
                    <tr key={i}>
                      <td style={{ fontWeight: 500 }}>{job.task_name || job.task}</td>
                      <td><span className={`tag ${job.type === 'mapreduce' ? 'tag-blue' : 'tag-violet'}`}>{job.type}</span></td>
                      <td>
                        <span className={`job-status ${job.status}`}>
                          <span className={`dot ${job.status === 'completed' ? 'green' : job.status === 'running' ? 'blue' : job.status === 'failed' ? 'red' : ''}`} />
                          {job.status}
                        </span>
                      </td>
                      <td style={{ color: 'var(--text-muted)', fontSize: 10, fontFamily: 'var(--font-mono)' }}>
                        {job.start_time?.slice(0, 19)?.replace('T', ' ')}
                      </td>
                      <td className="numeric">{job.duration ? `${job.duration}s` : '—'}</td>
                      <td className="numeric">{job.input_records?.toLocaleString() ?? '—'}</td>
                      <td className="numeric">{job.output_records?.toLocaleString() ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="state-message" style={{ padding: 'var(--sp-4)' }}>
                <div style={{ fontSize: 16, color: 'var(--text-muted)' }}>☰</div>
                <div className="state-desc">No jobs recorded yet. Run a MapReduce analysis or start a streaming session.</div>
              </div>
            )}
          </div>
        </div>

        {/* Mini Streaming Charts */}
        <div className="panel">
          <div className="panel-header">
            <span className="panel-title">Stream Monitor</span>
            <Link to="/workspace-b" className="btn btn-sm">Open Lab</Link>
          </div>
          <div className="panel-body" style={{ padding: 'var(--sp-1)', display: 'flex', flexDirection: 'column', gap: 'var(--sp-1)' }}>
            {miniChartData.length > 2 ? (
              <>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', padding: '0 var(--sp-1)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Req/Window</div>
                <div style={{ height: 60 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={miniChartData} margin={{ top: 2, right: 4, left: 0, bottom: 0 }}>
                      <Tooltip content={<MiniTooltip />} />
                      <Area type="monotone" dataKey="req" stroke={CHART_COLORS[1]} fill={CHART_COLORS[1]} fillOpacity={0.12} strokeWidth={1.5} dot={false} />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
                <div style={{ fontSize: 9, color: 'var(--text-muted)', padding: '0 var(--sp-1)', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Status Codes</div>
                <div style={{ height: 60 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={statusMini} margin={{ top: 2, right: 4, left: 0, bottom: 0 }}>
                      <Tooltip content={<MiniTooltip />} />
                      <Bar dataKey="2xx" stackId="a" fill="var(--status-2xx)" maxBarSize={8} />
                      <Bar dataKey="4xx" stackId="a" fill="var(--status-4xx)" maxBarSize={8} />
                      <Bar dataKey="5xx" stackId="a" fill="var(--status-5xx)" maxBarSize={8} radius={[1, 1, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </>
            ) : (
              <div className="state-message" style={{ padding: 'var(--sp-3)' }}>
                <div style={{ fontSize: 14, color: 'var(--text-muted)' }}>📡</div>
                <div className="state-desc">Start a streaming session to see live charts</div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function InfraRow({ label, ok, optional, port }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        <span className={`dot ${ok ? 'green' : optional ? 'blue' : 'red'}`} />
        <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)' }}>{label}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-1)' }}>
        {port && <span style={{ fontFamily: 'var(--font-mono)', fontSize: 9, color: 'var(--text-muted)' }}>:{port}</span>}
        <span className={`tag ${ok ? 'tag-green' : optional ? 'tag-blue' : 'tag-red'}`} style={{ fontSize: 9 }}>
          {ok ? 'ON' : optional ? 'IDLE' : 'OFF'}
        </span>
      </div>
    </div>
  )
}
