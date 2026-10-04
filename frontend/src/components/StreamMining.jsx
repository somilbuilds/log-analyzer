import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Legend, CartesianGrid,
} from 'recharts'

const tooltipStyle = {
  backgroundColor: '#0f141d',
  border: '1px solid #263142',
  borderRadius: 6,
  fontSize: 12,
  fontFamily: 'JetBrains Mono, monospace',
  color: '#e6edf6',
  padding: '8px 10px',
}

function num(value) {
  return Number(value || 0)
}

function fmt(value) {
  return num(value).toLocaleString()
}

function MetricRow({ label, value, tone }) {
  return (
    <div className="stat-row">
      <span className="stat-label">{label}</span>
      <span className={`stat-val ${tone || ''}`}>{value}</span>
    </div>
  )
}

export default function StreamMining({ bloom, dgim, fm }) {
  const bloomLatest = bloom?.length ? bloom[bloom.length - 1] : {}
  const dgimLatest = dgim?.length ? dgim[dgim.length - 1] : {}
  const fmLatest = fm?.length ? fm[fm.length - 1] : {}

  const dgimEstimate = num(dgimLatest.dgim_approximate_5xx)
  const dgimExact = num(dgimLatest.exact_5xx_total)
  const dgimAbsError = Math.abs(dgimEstimate - dgimExact)
  const dgimRelError = dgimExact ? (dgimAbsError / dgimExact) * 100 : 0

  const fmEstimate = num(fmLatest.fm_estimated_distinct_hosts)
  const fmExact = num(fmLatest.exact_distinct_hosts)
  const fmAbsError = Math.abs(fmEstimate - fmExact)
  const fmError = num(fmLatest.fm_error_pct) || (fmExact ? (fmAbsError / fmExact) * 100 : 0)

  const dgimData = (dgim || []).map((row) => ({
    ...row,
    absolute_error: Math.abs(num(row.dgim_approximate_5xx) - num(row.exact_5xx_total)),
  }))
  const fmData = (fm || []).map((row) => ({
    ...row,
    absolute_error: Math.abs(num(row.fm_estimated_distinct_hosts) - num(row.exact_distinct_hosts)),
  }))

  return (
    <section className="mining-grid">
      <div className="ops-panel algorithm-panel">
        <div className="panel-heading">
          <span className="algo-badge">New host detection</span>
          <h2>Bloom Filter</h2>
          <p>Question: which hosts are new to the stream, and how much false-positive risk has accumulated?</p>
        </div>
        <div className="stat-box">
          <MetricRow label="Hosts seen" value={fmt(bloomLatest.total_items_in_filter)} tone="green" />
          <MetricRow label="New hosts" value={fmt(bloomLatest.new_hosts)} tone="green" />
          <MetricRow label="Membership checks" value={fmt(bloomLatest.membership_checks || bloomLatest.total_checks)} />
          <MetricRow label="Estimated false-positive rate" value={`${(num(bloomLatest.estimated_fp_rate) * 100).toFixed(4)}%`} tone={num(bloomLatest.estimated_fp_rate) > 0.01 ? 'amber' : 'green'} />
        </div>
        <div className="chart-box short">
          <ResponsiveContainer>
            <BarChart data={bloom || []} margin={{ top: 10, right: 12, left: 0, bottom: 10 }}>
              <CartesianGrid stroke="#263142" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="window_id" tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} />
              <YAxis tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} width={48} />
              <Tooltip contentStyle={tooltipStyle} labelFormatter={(w) => `Window ${w}`} formatter={(v) => [Number(v).toLocaleString(), 'new hosts']} />
              <Bar dataKey="new_hosts" name="New hosts/window" fill="#31c48d" radius={[3, 3, 0, 0]} isAnimationActive={false} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="ops-panel algorithm-panel">
        <div className="panel-heading">
          <span className="algo-badge amber">Approximate counting</span>
          <h2>DGIM</h2>
          <p>Question: how close is the DGIM 5xx estimate to the exact count?</p>
        </div>
        <div className="stat-box">
          <MetricRow label="Sliding window size" value="5,000 requests" />
          <MetricRow label="DGIM estimate" value={fmt(dgimEstimate)} tone="amber" />
          <MetricRow label="Exact count" value={fmt(dgimExact)} tone="blue" />
          <MetricRow label="Absolute error" value={fmt(dgimAbsError)} />
          <MetricRow label="Relative error" value={`${dgimRelError.toFixed(2)}%`} tone={dgimRelError > 20 ? 'red' : 'green'} />
        </div>
        <div className="chart-box short">
          <ResponsiveContainer>
            <LineChart data={dgimData} margin={{ top: 10, right: 12, left: 0, bottom: 10 }}>
              <CartesianGrid stroke="#263142" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="window_id" tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} />
              <YAxis tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} width={48} />
              <Tooltip contentStyle={tooltipStyle} labelFormatter={(w) => `Window ${w}`} />
              <Legend wrapperStyle={{ color: '#aab6c8', fontSize: 12 }} />
              <Line type="monotone" dataKey="dgim_approximate_5xx" name="DGIM estimate" stroke="#f6c453" strokeWidth={2.2} strokeDasharray="5 4" dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="exact_5xx_total" name="Exact count" stroke="#4ea1ff" strokeWidth={2.2} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="ops-panel algorithm-panel">
        <div className="panel-heading">
          <span className="algo-badge purple">Distinct-host estimation</span>
          <h2>Flajolet-Martin</h2>
          <p>Question: how close is the probabilistic distinct-host estimate to the exact set size?</p>
        </div>
        <div className="stat-box">
          <MetricRow label="FM estimate" value={fmt(fmEstimate)} tone="purple" />
          <MetricRow label="Exact distinct count" value={fmt(fmExact)} tone="green" />
          <MetricRow label="Absolute error" value={fmt(fmAbsError)} />
          <MetricRow label="Percentage error" value={`${fmError.toFixed(2)}%`} tone={fmError > 20 ? 'red' : 'green'} />
        </div>
        <div className="chart-box short">
          <ResponsiveContainer>
            <LineChart data={fmData} margin={{ top: 10, right: 12, left: 0, bottom: 10 }}>
              <CartesianGrid stroke="#263142" strokeDasharray="3 3" vertical={false} />
              <XAxis dataKey="window_id" tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} />
              <YAxis tick={{ fill: '#96a3b6', fontSize: 12 }} tickLine={false} axisLine={{ stroke: '#2e3a4d' }} width={48} />
              <Tooltip contentStyle={tooltipStyle} labelFormatter={(w) => `Window ${w}`} />
              <Legend wrapperStyle={{ color: '#aab6c8', fontSize: 12 }} />
              <Line type="monotone" dataKey="fm_estimated_distinct_hosts" name="FM estimate" stroke="#9b8cff" strokeWidth={2.2} strokeDasharray="5 4" dot={false} isAnimationActive={false} />
              <Line type="monotone" dataKey="exact_distinct_hosts" name="Exact count" stroke="#31c48d" strokeWidth={2.2} dot={false} isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </section>
  )
}
