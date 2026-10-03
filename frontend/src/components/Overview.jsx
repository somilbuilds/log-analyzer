import { useEffect, useRef } from 'react'
import { AreaChart, Area, ResponsiveContainer } from 'recharts'

function fmtBytes(n) {
  if (!n) return '0 B'
  if (n > 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)} GB`
  if (n > 1_000_000) return `${(n / 1_000_000).toFixed(1)} MB`
  if (n > 1000) return `${(n / 1000).toFixed(1)} KB`
  return `${n} B`
}

function AnimatedValue({ value, suffix = '' }) {
  const ref = useRef(null)
  const prevRef = useRef(value)

  useEffect(() => {
    if (ref.current && prevRef.current !== value) {
      ref.current.style.animation = 'none'
      // eslint-disable-next-line no-void
      void ref.current.offsetWidth
      ref.current.style.animation = 'countUp 0.4s ease-out'
    }
    prevRef.current = value
  }, [value])

  return <span ref={ref}>{value}{suffix}</span>
}

export default function Overview({ metrics, live }) {
  const { aggregates, bloom } = metrics

  if (!aggregates || aggregates.length === 0) {
    return (
      <div className="empty-banner">
        <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>📡</div>
        <div>No windows yet. Hit <strong>Start stream</strong> to replay ClarkNet into the pipeline.</div>
      </div>
    )
  }

  const totalRequests = aggregates.reduce((sum, a) => sum + (a.total_requests || 0), 0)
  const avgErrorRate =
    (aggregates.reduce((sum, a) => sum + (a.error_rate || 0), 0) / aggregates.length) * 100
  const totalBytes = aggregates.reduce((sum, a) => sum + (a.total_bytes || 0), 0)
  const last = aggregates[aggregates.length - 1] || {}
  const uniqueHosts =
    bloom && bloom.length > 0
      ? (bloom[bloom.length - 1].total_items_in_filter || 0).toLocaleString()
      : '—'

  let anomalyCount = 0
  if (aggregates.length > 2) {
    const mean = totalRequests / aggregates.length
    const std = Math.sqrt(
      aggregates.reduce((sq, n) => sq + ((n.total_requests || 0) - mean) ** 2, 0) /
        (aggregates.length - 1)
    )
    aggregates.forEach((agg) => {
      if ((agg.total_requests || 0) > mean + 2 * std) anomalyCount++
      if ((agg.error_rate || 0) > 0.1) anomalyCount++
    })
  }

  // Mini sparkline data (last 20 windows)
  const sparkData = aggregates.slice(-20).map((a) => ({ v: a.total_requests || 0 }))
  const errorSparkData = aggregates.slice(-20).map((a) => ({ v: (a.error_rate || 0) * 100 }))

  const kpis = [
    {
      label: 'Total requests',
      value: totalRequests.toLocaleString(),
      hint: 'sum of all windows',
      sparkline: sparkData,
      color: '#60a5fa',
    },
    {
      label: 'This window',
      value: (last.total_requests || 0).toLocaleString(),
      hint: live ? 'latest micro-batch' : 'last completed',
      sparkline: sparkData,
      color: '#22d3ee',
    },
    {
      label: 'Error rate',
      value: `${avgErrorRate.toFixed(2)}%`,
      hint: '4xx + 5xx / total',
      sparkline: errorSparkData,
      color: '#f87171',
    },
    {
      label: 'Unique hosts',
      value: uniqueHosts,
      hint: 'Bloom filter membership',
      color: '#a78bfa',
    },
    {
      label: 'Data volume',
      value: fmtBytes(totalBytes),
      hint: 'total response bytes',
      color: '#34d399',
    },
    {
      label: 'Anomalies',
      value: anomalyCount,
      hint: 'traffic > μ+2σ or errors > 10%',
      alert: anomalyCount > 0,
      color: '#f87171',
    },
  ]

  return (
    <div className="overview-grid">
      {kpis.map((kpi) => (
        <div key={kpi.label} className={`kpi-card ${kpi.alert ? 'alert-card' : ''}`}>
          <div className="kpi-label">{kpi.label}</div>
          <div className={`kpi-value ${kpi.alert ? 'alert' : ''}`}>
            <AnimatedValue value={kpi.value} />
          </div>
          <div className="kpi-hint">{kpi.hint}</div>
          {kpi.sparkline && kpi.sparkline.length > 2 && (
            <div className="kpi-sparkline">
              <ResponsiveContainer>
                <AreaChart data={kpi.sparkline} margin={{ top: 2, right: 0, left: 0, bottom: 0 }}>
                  <Area
                    type="monotone"
                    dataKey="v"
                    stroke={kpi.color}
                    fill={kpi.color}
                    fillOpacity={0.12}
                    strokeWidth={1.5}
                    dot={false}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      ))}
    </div>
  )
}
