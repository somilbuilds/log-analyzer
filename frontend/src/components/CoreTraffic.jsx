import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Legend,
} from 'recharts'

const tooltipStyle = {
  backgroundColor: '#0f141d',
  border: '1px solid #263142',
  borderRadius: 6,
  color: '#e6edf6',
  fontFamily: 'JetBrains Mono, monospace',
  fontSize: 12,
  padding: '8px 10px',
}

export default function CoreTraffic({ aggregates }) {
  if (!aggregates || aggregates.length === 0) return null

  const data = aggregates.map((a) => ({
    window: a.window_id,
    requests: a.total_requests || 0,
    errorPct: Number(((a.error_rate || 0) * 100).toFixed(2)),
  }))

  const mean = data.reduce((a, b) => a + b.requests, 0) / data.length
  const errorMean = data.reduce((a, b) => a + b.errorPct, 0) / data.length
  const std = Math.sqrt(data.reduce((sq, n) => sq + (n.requests - mean) ** 2, 0) / (data.length - 1 || 1))
  const threshold = mean + 2 * std

  const feed = [...data].reverse().flatMap((d) => {
    const rows = []
    if (d.requests > threshold && std > 0) {
      rows.push({ window: d.window, label: 'Traffic spike', value: `${d.requests.toLocaleString()} requests`, detail: `above μ+2σ (${Math.round(threshold).toLocaleString()})`, severity: 'critical' })
    }
    if (d.errorPct > 10) {
      rows.push({ window: d.window, label: 'Error surge', value: `${d.errorPct.toFixed(1)}%`, detail: 'above 10% warning threshold', severity: 'warning' })
    }
    return rows
  })

  return (
    <section className="traffic-grid">
      <div className="ops-panel">
        <div className="panel-heading">
          <h2>Request Rate</h2>
          <p>Question: how many HTTP requests arrive in each Spark micro-batch window?</p>
        </div>
        <div className="chart-box tall">
          <ResponsiveContainer>
            <LineChart data={data} margin={{ top: 12, right: 20, left: 8, bottom: 16 }}>
              <CartesianGrid stroke="#263142" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="window" tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} label={{ value: 'Window', fill: '#96a3b6', fontSize: 12, position: 'insideBottom' }} />
              <YAxis tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} tickFormatter={(v) => v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v} width={52} />
              <Tooltip contentStyle={tooltipStyle} labelFormatter={(w) => `Window ${w}`} formatter={(v) => [Number(v).toLocaleString(), 'requests']} cursor={{ stroke: '#5f6f85' }} />
              <Legend wrapperStyle={{ color: '#aab6c8', fontSize: 12 }} />
              <ReferenceLine y={mean} stroke="#8a96a8" strokeDasharray="5 5" label={{ value: `mean ${Math.round(mean).toLocaleString()}`, fill: '#aab6c8', fontSize: 12, position: 'insideTopLeft' }} />
              <ReferenceLine y={threshold} stroke="#f97373" strokeDasharray="5 5" label={{ value: 'μ + 2σ', fill: '#f97373', fontSize: 12, position: 'insideTopRight' }} />
              <Line type="monotone" dataKey="requests" name="Requests/window" stroke="#4ea1ff" strokeWidth={2.4} dot={{ r: 2, fill: '#4ea1ff' }} activeDot={{ r: 5 }} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="panel-heading subheading">
          <h2>Error Rate</h2>
          <p>Question: what share of each window returned 4xx or 5xx responses?</p>
        </div>
        <div className="chart-box short">
          <ResponsiveContainer>
            <LineChart data={data} margin={{ top: 10, right: 20, left: 8, bottom: 16 }}>
              <CartesianGrid stroke="#263142" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="window" tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} />
              <YAxis tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} unit="%" width={52} />
              <Tooltip contentStyle={tooltipStyle} formatter={(v) => [`${v}%`, 'error rate']} cursor={{ stroke: '#5f6f85' }} />
              <ReferenceLine y={errorMean} stroke="#8a96a8" strokeDasharray="5 5" label={{ value: `mean ${errorMean.toFixed(1)}%`, fill: '#aab6c8', fontSize: 12, position: 'insideTopLeft' }} />
              <ReferenceLine y={10} stroke="#f6c453" strokeDasharray="5 5" label={{ value: '10% warning', fill: '#f6c453', fontSize: 12, position: 'insideTopRight' }} />
              <Line type="monotone" dataKey="errorPct" name="Error rate" stroke="#f97373" strokeWidth={2.2} dot={{ r: 2, fill: '#f97373' }} activeDot={{ r: 5 }} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="ops-panel">
        <div className="panel-heading">
          <h2>Anomaly Feed</h2>
          <p>Newest threshold crossings across processed windows.</p>
        </div>
        <div className="anomaly-feed">
          {feed.length === 0 ? (
            <div className="empty-inline">No window has crossed the traffic or error thresholds.</div>
          ) : (
            feed.slice(0, 14).map((item, index) => (
              <div className={`anomaly-card ${item.severity === 'warning' ? 'warning' : ''}`} key={`${item.window}-${item.label}-${index}`}>
                <div>
                  <div className="anomaly-type">{item.label}</div>
                  <div className="anomaly-metric">{item.value} · {item.detail}</div>
                </div>
                <div className="anomaly-time">W{item.window}</div>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
  )
}
