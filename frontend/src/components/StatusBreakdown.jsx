const STATUS_COLORS = {
  '2xx': '#31c48d',
  '3xx': '#4ea1ff',
  '4xx': '#f6c453',
  '5xx': '#f97373',
}

function pct(count, total) {
  if (!total) return '0.0%'
  return `${((count / total) * 100).toFixed(1)}%`
}

function TopTable({ title, description, rows, nameKey }) {
  const total = rows.reduce((sum, row) => sum + row.reqs, 0) || 1
  return (
    <section className="ops-panel table-panel">
      <div className="panel-heading">
        <h2>{title}</h2>
        <p>{description}</p>
      </div>
      <div className="analytic-table">
        <div className="analytic-row analytic-head">
          <span>Rank</span>
          <span>{nameKey}</span>
          <span>Requests</span>
          <span>%</span>
        </div>
        {rows.map((row, index) => (
          <div className="analytic-row" key={`${row.name}-${index}`}>
            <span className="mono dim">{String(index + 1).padStart(2, '0')}</span>
            <span className="table-name" title={row.name}>{row.name}</span>
            <span className="mono">{row.reqs.toLocaleString()}</span>
            <span className="mono dim">{pct(row.reqs, total)}</span>
          </div>
        ))}
      </div>
    </section>
  )
}

export default function StatusBreakdown({ aggregates }) {
  if (!aggregates || aggregates.length === 0) return null

  const hostTotals = {}
  const pathTotals = {}
  const statusTotals = { '2xx': 0, '3xx': 0, '4xx': 0, '5xx': 0 }

  aggregates.forEach((agg) => {
    ;(agg.top_hosts || []).forEach((h) => {
      hostTotals[h.host] = (hostTotals[h.host] || 0) + h.count
    })
    ;(agg.top_paths || []).forEach((p) => {
      pathTotals[p.path] = (pathTotals[p.path] || 0) + p.count
    })
    Object.entries(agg.status_distribution || {}).forEach(([code, count]) => {
      const value = Number(count) || 0
      if (String(code).startsWith('2')) statusTotals['2xx'] += value
      else if (String(code).startsWith('3')) statusTotals['3xx'] += value
      else if (String(code).startsWith('4')) statusTotals['4xx'] += value
      else if (String(code).startsWith('5')) statusTotals['5xx'] += value
    })
  })

  const top = (totals, limit = 10) => Object.entries(totals)
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([name, reqs]) => ({ name, reqs }))

  const statusData = [
    { label: '2xx Success', key: '2xx', count: statusTotals['2xx'] },
    { label: '3xx Redirect', key: '3xx', count: statusTotals['3xx'] },
    { label: '4xx Client error', key: '4xx', count: statusTotals['4xx'] },
    { label: '5xx Server error', key: '5xx', count: statusTotals['5xx'] },
  ]
  const statusTotal = statusData.reduce((sum, row) => sum + row.count, 0) || 1

  return (
    <section className="status-section">
      <section className="ops-panel">
        <div className="panel-heading">
          <h2>HTTP Status</h2>
          <p>Question: how much of the processed stream is success, redirect, client error, or server error?</p>
        </div>
        <div className="status-bars">
          {statusData.map((row) => {
            const width = `${Math.max((row.count / statusTotal) * 100, row.count ? 2 : 0)}%`
            return (
              <div className={`status-bar-row ${row.key === '5xx' ? 'server-error' : ''}`} key={row.key}>
                <span className="status-family">{row.key}</span>
                <span>{row.label}</span>
                <strong className="mono">{row.count.toLocaleString()}</strong>
                <span className="mono dim">{pct(row.count, statusTotal)}</span>
                <div className="status-track"><div style={{ width, background: STATUS_COLORS[row.key] }} /></div>
              </div>
            )
          })}
        </div>
      </section>

      <TopTable title="Top Hosts" description="Readable hostnames from ClarkNet clients across processed windows." rows={top(hostTotals)} nameKey="Host" />
      <TopTable title="Top Endpoints" description="Most requested paths across processed windows." rows={top(pathTotals)} nameKey="Endpoint" />
    </section>
  )
}
