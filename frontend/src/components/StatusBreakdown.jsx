import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell,
  PieChart, Pie, Cell as PieCell,
} from 'recharts'

const tooltipStyle = {
  backgroundColor: 'rgba(14, 18, 27, 0.95)',
  padding: '8px 12px',
  border: '1px solid rgba(255,255,255,0.1)',
  borderRadius: 10,
  fontFamily: 'JetBrains Mono, monospace',
  fontSize: 11,
  color: '#e0e4ea',
  boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
}

function shortName(v, n = 22) {
  if (!v) return ''
  return v.length > n ? `${v.slice(0, n - 1)}…` : v
}

const STATUS_COLORS = {
  '2xx': '#34d399',
  '3xx': '#60a5fa',
  '4xx': '#fbbf24',
  '5xx': '#f87171',
}

export default function StatusBreakdown({ aggregates }) {
  if (!aggregates || aggregates.length === 0) return null

  const hostTotals = {}
  const pathTotals = {}
  const statusTotals = { '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 }

  aggregates.forEach((agg) => {
    (agg.top_hosts || []).forEach((h) => {
      hostTotals[h.host] = (hostTotals[h.host] || 0) + h.count
    })
    ;(agg.top_paths || []).forEach((p) => {
      pathTotals[p.path] = (pathTotals[p.path] || 0) + p.count
    })
    if (agg.status_distribution) {
      Object.entries(agg.status_distribution).forEach(([code, count]) => {
        const n = Number(count)
        if (String(code).startsWith('2')) statusTotals['2xx'] += n
        else if (String(code).startsWith('3')) statusTotals['3xx'] += n
        else if (String(code).startsWith('4')) statusTotals['4xx'] += n
        else if (String(code).startsWith('5')) statusTotals['5xx'] += n
      })
    }
  })

  const top = (totals, limit = 10) =>
    Object.entries(totals)
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([name, reqs]) => ({ name, reqs }))

  const statusData = [
    { name: '2xx OK', count: statusTotals['2xx'], fill: STATUS_COLORS['2xx'] },
    { name: '3xx Redirect', count: statusTotals['3xx'], fill: STATUS_COLORS['3xx'] },
    { name: '4xx Client', count: statusTotals['4xx'], fill: STATUS_COLORS['4xx'] },
    { name: '5xx Server', count: statusTotals['5xx'], fill: STATUS_COLORS['5xx'] },
  ]
  const total = statusData.reduce((s, d) => s + d.count, 0) || 1

  const CustomTooltip = ({ active, payload }) => {
    if (!active || !payload?.length) return null
    const row = payload[0]
    return (
      <div style={tooltipStyle}>
        <div style={{ fontWeight: 600 }}>{row.payload.name}</div>
        <div>{Number(row.value).toLocaleString()} requests</div>
        {row.payload.count !== undefined && (
          <div style={{ color: '#8b95a8', marginTop: 2 }}>
            {((row.payload.count / total) * 100).toFixed(1)}% of total
          </div>
        )}
      </div>
    )
  }

  const BarLabel = ({ x, y, width, value }) => {
    if (width < 30) return null
    return (
      <text x={x + width - 6} y={y + 14} fill="#8b95a8" fontSize={9} textAnchor="end" fontFamily="JetBrains Mono">
        {value >= 1000 ? `${(value/1000).toFixed(1)}k` : value}
      </text>
    )
  }

  return (
    <div className="three-col">
      <div className="card">
        <h3 className="section-title">HTTP status mix</h3>
        <p className="chart-help">Distribution of response codes across all processed windows.</p>
        <div className="chart-box mid">
          <ResponsiveContainer>
            <BarChart data={statusData} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 0 }}>
              <XAxis type="number" hide />
              <YAxis
                dataKey="name"
                type="category"
                width={90}
                tick={{ fill: '#8b95a8', fontSize: 11, fontWeight: 500 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.02)' }} />
              <Bar dataKey="count" radius={[0, 6, 6, 0]} isAnimationActive={true} animationDuration={600} label={<BarLabel />}>
                {statusData.map((d) => (
                  <Cell key={d.name} fill={d.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="status-legend">
          {statusData.map((d) => (
            <span key={d.name} style={{ display: 'flex', alignItems: 'center', gap: '0.25rem' }}>
              <span className="legend-dot" style={{ background: d.fill }} />
              {d.name}: {((d.count / total) * 100).toFixed(1)}%
            </span>
          ))}
        </div>
      </div>

      <div className="card">
        <h3 className="section-title">Top endpoints</h3>
        <p className="chart-help">Most requested paths from ClarkNet traces (plus injected DDoS targets).</p>
        <div className="chart-box mid">
          <ResponsiveContainer>
            <BarChart data={top(pathTotals)} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 0 }}>
              <XAxis type="number" hide />
              <YAxis
                dataKey="name"
                type="category"
                width={115}
                tick={{ fill: '#8b95a8', fontSize: 10 }}
                tickFormatter={(v) => shortName(v, 16)}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.02)' }} />
              <Bar
                dataKey="reqs"
                fill="#60a5fa"
                radius={[0, 6, 6, 0]}
                isAnimationActive={true}
                animationDuration={600}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="card">
        <h3 className="section-title">Top hosts</h3>
        <p className="chart-help">Client hostnames from the original 1995 ClarkNet traces.</p>
        <div className="chart-box mid">
          <ResponsiveContainer>
            <BarChart data={top(hostTotals)} layout="vertical" margin={{ top: 4, right: 16, left: 8, bottom: 0 }}>
              <XAxis type="number" hide />
              <YAxis
                dataKey="name"
                type="category"
                width={115}
                tick={{ fill: '#8b95a8', fontSize: 10 }}
                tickFormatter={(v) => shortName(v, 16)}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(255,255,255,0.02)' }} />
              <Bar
                dataKey="reqs"
                fill="#22d3ee"
                radius={[0, 6, 6, 0]}
                isAnimationActive={true}
                animationDuration={600}
              />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}
