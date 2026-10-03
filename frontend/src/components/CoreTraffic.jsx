import {
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Legend,
} from 'recharts'

const tooltipStyle = {
  backgroundColor: 'rgba(14, 18, 27, 0.95)',
  border: '1px solid rgba(255,255,255,0.1)',
  borderRadius: 10,
  color: '#e0e4ea',
  fontFamily: 'JetBrains Mono, monospace',
  fontSize: 12,
  backdropFilter: 'blur(12px)',
  padding: '8px 12px',
  boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
}

function CustomDot({ cx, cy, payload, threshold }) {
  if (!payload || (payload.requests || 0) <= threshold) return null
  return (
    <g>
      <circle cx={cx} cy={cy} r={6} fill="rgba(248,113,113,0.2)" />
      <circle cx={cx} cy={cy} r={3} fill="#f87171" />
    </g>
  )
}

export default function CoreTraffic({ aggregates }) {
  if (!aggregates || aggregates.length === 0) return null

  const data = aggregates.map((a) => ({
    window: a.window_id,
    requests: a.total_requests || 0,
    errorPct: Number(((a.error_rate || 0) * 100).toFixed(2)),
  }))

  const mean = data.reduce((a, b) => a + b.requests, 0) / data.length
  const std = Math.sqrt(
    data.reduce((sq, n) => sq + (n.requests - mean) ** 2, 0) / (data.length - 1 || 1)
  )
  const threshold = mean + 2 * std

  const feed = []
  ;[...data].reverse().forEach((d) => {
    if (d.requests > threshold && std > 0) {
      feed.push({
        window: d.window,
        type: '🔴 Traffic spike',
        why: `${d.requests.toLocaleString()} reqs vs μ+2σ = ${Math.round(threshold)}`,
        severity: 'critical',
      })
    }
    if (d.errorPct > 10) {
      feed.push({
        window: d.window,
        type: '⚠️ Error surge',
        why: `${d.errorPct.toFixed(1)}% of window failed`,
        severity: 'warning',
      })
    }
  })

  return (
    <div className="traffic-grid">
      <div className="card">
        <h3 className="section-title">Requests per window</h3>
        <p className="chart-help">
          Each point is one micro-batch window. The dashed line shows the mean; glowing red dots mark anomalies above μ+2σ.
        </p>
        <div className="chart-box tall">
          <ResponsiveContainer>
            <AreaChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="reqFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#60a5fa" stopOpacity={0.35} />
                  <stop offset="50%" stopColor="#3b82f6" stopOpacity={0.12} />
                  <stop offset="100%" stopColor="#3b82f6" stopOpacity={0.01} />
                </linearGradient>
                <filter id="glow">
                  <feGaussianBlur stdDeviation="3" result="coloredBlur" />
                  <feMerge>
                    <feMergeNode in="coloredBlur" />
                    <feMergeNode in="SourceGraphic" />
                  </feMerge>
                </filter>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
              <XAxis
                dataKey="window"
                tick={{ fill: '#5b6474', fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                interval="preserveStartEnd"
              />
              <YAxis
                tick={{ fill: '#5b6474', fontSize: 10 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={(v) => v >= 1000 ? `${(v/1000).toFixed(0)}k` : v}
              />
              <Tooltip
                contentStyle={tooltipStyle}
                labelFormatter={(w) => `Window ${w}`}
                cursor={{ stroke: 'rgba(255,255,255,0.06)' }}
              />
              <Legend wrapperStyle={{ fontSize: 11, color: '#8b95a8' }} />
              <ReferenceLine
                y={mean}
                stroke="#4f5a6b"
                strokeDasharray="6 4"
                label={{ value: `μ = ${Math.round(mean)}`, fill: '#5b6474', fontSize: 10, position: 'insideTopLeft' }}
              />
              <ReferenceLine
                y={threshold}
                stroke="#f87171"
                strokeDasharray="3 3"
                strokeOpacity={0.4}
                label={{ value: 'μ+2σ', fill: '#f87171', fontSize: 10, position: 'insideTopRight' }}
              />
              <Area
                type="monotone"
                dataKey="requests"
                name="Requests"
                stroke="#60a5fa"
                fill="url(#reqFill)"
                strokeWidth={2}
                dot={<CustomDot threshold={threshold} />}
                activeDot={{ r: 5, fill: '#60a5fa', strokeWidth: 0 }}
                isAnimationActive={true}
                animationDuration={800}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>

        <h3 className="section-title" style={{ marginTop: '1.4rem' }}>Error rate (%)</h3>
        <p className="chart-help">Share of 4xx and 5xx responses in each window. The amber line marks the 10% warning threshold.</p>
        <div className="chart-box short">
          <ResponsiveContainer>
            <AreaChart data={data} margin={{ top: 8, right: 12, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="errorFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#f87171" stopOpacity={0.25} />
                  <stop offset="100%" stopColor="#f87171" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
              <XAxis dataKey="window" tick={{ fill: '#5b6474', fontSize: 10 }} axisLine={false} tickLine={false} interval="preserveStartEnd" />
              <YAxis tick={{ fill: '#5b6474', fontSize: 10 }} axisLine={false} tickLine={false} unit="%" />
              <Tooltip contentStyle={tooltipStyle} formatter={(v) => [`${v}%`, 'Error rate']} cursor={{ stroke: 'rgba(255,255,255,0.06)' }} />
              <ReferenceLine y={10} stroke="#fbbf24" strokeDasharray="4 4" strokeOpacity={0.5} />
              <Area
                type="monotone"
                dataKey="errorPct"
                name="Error %"
                stroke="#f87171"
                fill="url(#errorFill)"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: '#f87171' }}
                isAnimationActive={true}
                animationDuration={800}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="card">
        <h3 className="section-title">Anomaly feed</h3>
        <p className="chart-help">Newest detections first. Spikes use volume; surges use error rate.</p>
        <div className="anomaly-feed">
          {feed.length === 0 ? (
            <div className="empty-inline">No window has crossed the thresholds yet.</div>
          ) : (
            feed.slice(0, 14).map((a, i) => (
              <div
                key={i}
                className={`anomaly-card ${a.severity === 'warning' ? 'warning' : ''}`}
                style={{ animationDelay: `${i * 50}ms` }}
              >
                <div>
                  <div className="anomaly-type">{a.type}</div>
                  <div className="anomaly-metric">{a.why}</div>
                </div>
                <div className="anomaly-time">W{a.window}</div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  )
}
