import {
  AreaChart, Area, LineChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Legend, CartesianGrid,
} from 'recharts'

const tooltipStyle = {
  backgroundColor: 'rgba(14, 18, 27, 0.95)',
  border: '1px solid rgba(255,255,255,0.1)',
  borderRadius: 10,
  fontSize: 12,
  fontFamily: 'JetBrains Mono, monospace',
  color: '#e0e4ea',
  padding: '8px 12px',
  boxShadow: '0 8px 32px rgba(0,0,0,0.4)',
}

export default function StreamMining({ bloom, dgim, fm }) {
  const bloomLatest = bloom?.length ? bloom[bloom.length - 1] : {}
  const dgimLatest = dgim?.length ? dgim[dgim.length - 1] : {}
  const fmLatest = fm?.length ? fm[fm.length - 1] : {}

  return (
    <div className="three-col">
      {/* ─── BLOOM FILTER ─── */}
      <div className="card">
        <span className="algo-badge">Set membership</span>
        <h3 className="algo-title">Bloom Filter</h3>
        <p className="chart-help">
          Fixed-size bit array answers "have we seen this host?" New host arrivals decay as the trace replays returning clients.
        </p>
        <div className="stat-box">
          <div className="stat-row">
            <span className="stat-label">New hosts (this window)</span>
            <span className="stat-val green">{(bloomLatest.new_hosts || 0).toLocaleString()}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Total in filter</span>
            <span className="stat-val">{(bloomLatest.total_items_in_filter || 0).toLocaleString()}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Est. false-positive rate</span>
            <span className="stat-val" style={{ color: (bloomLatest.estimated_fp_rate || 0) > 0.01 ? 'var(--amber)' : 'var(--green)' }}>
              {((bloomLatest.estimated_fp_rate || 0) * 100).toFixed(4)}%
            </span>
          </div>
        </div>
        <div className="chart-box short">
          <ResponsiveContainer>
            <AreaChart data={bloom || []} margin={{ top: 6, right: 4, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="bloomFill" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#34d399" stopOpacity={0.3} />
                  <stop offset="100%" stopColor="#34d399" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.03)" vertical={false} />
              <XAxis dataKey="window_id" hide />
              <YAxis tick={{ fill: '#5b6474', fontSize: 10 }} width={32} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={tooltipStyle} labelFormatter={(w) => `Window ${w}`} />
              <Area
                type="monotone"
                dataKey="new_hosts"
                name="New hosts"
                stroke="#34d399"
                fill="url(#bloomFill)"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: '#34d399' }}
                isAnimationActive={true}
                animationDuration={600}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ─── DGIM ─── */}
      <div className="card">
        <span className="algo-badge">Counting 1s</span>
        <h3 className="algo-title">DGIM</h3>
        <p className="chart-help">
          Approximate count of 5xx bits in a sliding window of 5,000 requests. Dashed = DGIM estimate, solid = exact running total.
        </p>
        <div className="stat-box">
          <div className="stat-row">
            <span className="stat-label">DGIM ≈ 5xx (window)</span>
            <span className="stat-val amber">{(dgimLatest.dgim_approximate_5xx || 0).toLocaleString()}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Exact 5xx (all time)</span>
            <span className="stat-val cyan">{(dgimLatest.exact_5xx_total || 0).toLocaleString()}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Buckets</span>
            <span className="stat-val">{dgimLatest.dgim_num_buckets || 0}</span>
          </div>
        </div>
        <div className="chart-box short">
          <ResponsiveContainer>
            <LineChart data={dgim || []} margin={{ top: 6, right: 4, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.03)" vertical={false} />
              <XAxis dataKey="window_id" hide />
              <YAxis tick={{ fill: '#5b6474', fontSize: 10 }} width={36} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={tooltipStyle} labelFormatter={(w) => `Window ${w}`} />
              <Legend wrapperStyle={{ fontSize: 10, color: '#8b95a8' }} />
              <Line
                type="monotone"
                dataKey="dgim_approximate_5xx"
                name="DGIM ≈"
                stroke="#fbbf24"
                strokeWidth={2}
                strokeDasharray="5 3"
                dot={false}
                activeDot={{ r: 4, fill: '#fbbf24' }}
                isAnimationActive={true}
                animationDuration={600}
              />
              <Line
                type="monotone"
                dataKey="exact_5xx_total"
                name="Exact"
                stroke="#60a5fa"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: '#60a5fa' }}
                isAnimationActive={true}
                animationDuration={600}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* ─── FLAJOLET-MARTIN ─── */}
      <div className="card">
        <span className="algo-badge">Distinct count</span>
        <h3 className="algo-title">Flajolet–Martin</h3>
        <p className="chart-help">
          Distinct host count from max trailing zeros in hashes, vs the exact set. Error = |FM − exact| / exact.
        </p>
        <div className="stat-box">
          <div className="stat-row">
            <span className="stat-label">FM estimate</span>
            <span className="stat-val purple">{(fmLatest.fm_estimated_distinct_hosts || 0).toLocaleString()}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Exact distinct</span>
            <span className="stat-val green">{(fmLatest.exact_distinct_hosts || 0).toLocaleString()}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Error</span>
            <span className="stat-val" style={{ color: (fmLatest.fm_error_pct || 0) > 20 ? 'var(--red)' : 'var(--text)' }}>
              {(fmLatest.fm_error_pct || 0).toFixed(1)}%
            </span>
          </div>
        </div>
        <div className="chart-box short">
          <ResponsiveContainer>
            <LineChart data={fm || []} margin={{ top: 6, right: 4, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.03)" vertical={false} />
              <XAxis dataKey="window_id" hide />
              <YAxis tick={{ fill: '#5b6474', fontSize: 10 }} width={40} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={tooltipStyle} labelFormatter={(w) => `Window ${w}`} />
              <Legend wrapperStyle={{ fontSize: 10, color: '#8b95a8' }} />
              <Line
                type="monotone"
                dataKey="fm_estimated_distinct_hosts"
                name="FM estimate"
                stroke="#a78bfa"
                strokeWidth={2}
                strokeDasharray="5 3"
                dot={false}
                activeDot={{ r: 4, fill: '#a78bfa' }}
                isAnimationActive={true}
                animationDuration={600}
              />
              <Line
                type="monotone"
                dataKey="exact_distinct_hosts"
                name="Exact"
                stroke="#34d399"
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4, fill: '#34d399' }}
                isAnimationActive={true}
                animationDuration={600}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  )
}
