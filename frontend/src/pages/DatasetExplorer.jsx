import { usePolling, formatBytes, formatNumber } from '../hooks/useApi'

export default function DatasetExplorer() {
  const { data: dataset } = usePolling('/dataset', 15000)
  const { data: hdfs } = usePolling('/hdfs/status', 10000)
  const { data: fsck } = usePolling('/hdfs/fsck', 15000)
  const { data: report } = usePolling('/hdfs/report', 15000)

  const info = dataset || {}
  const files = hdfs?.files || []
  const summary = report?.summary || {}
  const nodes = report?.nodes || []

  return (
    <div className="workspace">
      {/* Dataset Info */}
      <div className="metric-strip">
        <div className="metric-card">
          <div className="metric-label">Source</div>
          <div className="metric-value" style={{ fontSize: 'var(--text-sm)' }}>{info.source || 'ClarkNet'}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Format</div>
          <div className="metric-value" style={{ fontSize: 'var(--text-sm)' }}>{info.format || 'CLF'}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Records</div>
          <div className="metric-value teal">{formatNumber(info.lines)}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">File Size</div>
          <div className="metric-value blue">{formatBytes(info.size_bytes)}</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Date Range</div>
          <div className="metric-value" style={{ fontSize: 'var(--text-xs)' }}>
            {info.date_range?.first?.split(':')[0] || '—'} → {info.date_range?.last?.split(':')[0] || ''}
          </div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS</div>
          <div className={`metric-value ${hdfs?.dataset_ready ? 'green' : 'amber'}`}>
            {hdfs?.dataset_ready ? 'Ready' : 'Not Loaded'}
          </div>
        </div>
      </div>

      <div className="workspace-body">
        {/* CLF Schema */}
        <div className="panel" style={{ width: 260, flexShrink: 0 }}>
          <div className="panel-header">
            <span className="panel-title">CLF Schema</span>
          </div>
          <div className="panel-body">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
              {(info.fields || ['host', 'ident', 'authuser', 'timestamp', 'request', 'status', 'bytes']).map(field => (
                <div key={field} style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--accent-cyan)' }}>{field}</span>
                  <span className="tag tag-blue">{fieldType(field)}</span>
                </div>
              ))}
            </div>
            <div style={{ marginTop: 'var(--sp-4)', borderTop: '1px solid var(--border-default)', paddingTop: 'var(--sp-3)' }}>
              <div className="panel-title" style={{ marginBottom: 'var(--sp-2)' }}>HDFS Storage</div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-1)' }}>
                <InfoRow label="Total Blocks" value={fsck?.total_blocks ?? '—'} />
                <InfoRow label="Replication" value={fsck?.replication_factor ? `${fsck.replication_factor}×` : '—'} />
                <InfoRow label="Avg Replication" value={fsck?.avg_replication ?? '—'} />
                <InfoRow label="Dataset Size" value={formatBytes(fsck?.total_size)} />
                <InfoRow label="Under-repl" value={fsck?.under_replicated ?? 0} />
              </div>
            </div>
          </div>
        </div>

        {/* HDFS File Browser */}
        <div className="panel flex-1">
          <div className="panel-header">
            <span className="panel-title">HDFS Files</span>
            <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-muted)' }}>/user/data/</span>
          </div>
          <div className="panel-body-flush" style={{ overflow: 'auto' }}>
            {files.length === 0 ? (
              <div className="state-message">
                <div className="state-icon">⬡</div>
                <div className="state-title">No HDFS Files</div>
                <div className="state-desc">HDFS may be unavailable or the dataset hasn't been uploaded yet.</div>
              </div>
            ) : (
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Path</th>
                    <th>Type</th>
                    <th className="numeric">Size</th>
                    <th className="numeric">Repl</th>
                    <th>Owner</th>
                    <th>Date</th>
                    <th>Permissions</th>
                  </tr>
                </thead>
                <tbody>
                  {files.map((f, i) => (
                    <tr key={i}>
                      <td style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: f.is_dir ? 'var(--accent-cyan)' : 'var(--text-primary)' }}>
                        {f.path}
                      </td>
                      <td>
                        <span className={`tag ${f.is_dir ? 'tag-blue' : 'tag-teal'}`}>{f.is_dir ? 'dir' : 'file'}</span>
                      </td>
                      <td className="numeric">{f.is_dir ? '—' : formatBytes(parseInt(f.size) || 0)}</td>
                      <td className="numeric">{f.replication}</td>
                      <td style={{ color: 'var(--text-muted)' }}>{f.owner}</td>
                      <td style={{ fontSize: 10, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{f.date}</td>
                      <td style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)' }}>{f.permissions}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {/* DataNodes */}
        <div className="panel" style={{ width: 220, flexShrink: 0 }}>
          <div className="panel-header">
            <span className="panel-title">DataNodes</span>
            <span style={{ fontSize: 10, color: 'var(--text-muted)' }}>{nodes.length} nodes</span>
          </div>
          <div className="panel-body">
            {nodes.length === 0 ? (
              <div className="state-message">
                <div className="state-desc">No DataNodes reported</div>
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
                {nodes.map((node, i) => (
                  <div key={i} style={{ borderBottom: '1px solid var(--border-default)', paddingBottom: 'var(--sp-2)' }}>
                    <div style={{ fontWeight: 600, fontSize: 'var(--text-xs)', color: 'var(--accent-cyan)', marginBottom: 'var(--sp-1)' }}>
                      {node.hostname || node.address}
                    </div>
                    <InfoRow label="Capacity" value={formatBytes(node.capacity)} />
                    <InfoRow label="Used" value={formatBytes(node.used)} />
                    <InfoRow label="Remaining" value={formatBytes(node.remaining)} />
                    <InfoRow label="Blocks" value={node.num_blocks ?? '—'} />
                    <div className="progress-bar" style={{ marginTop: 'var(--sp-1)' }}>
                      <div className="progress-fill blue" style={{ width: `${node.used_pct || 0}%` }} />
                    </div>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
                      {node.used_pct ? `${node.used_pct}% used` : ''}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

function fieldType(field) {
  const types = { host: 'string', ident: 'string', authuser: 'string', timestamp: 'datetime', request: 'string', status: 'int', bytes: 'int' }
  return types[field] || 'string'
}

function InfoRow({ label, value }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 'var(--text-xs)', lineHeight: 1.7 }}>
      <span style={{ color: 'var(--text-muted)' }}>{label}</span>
      <span style={{ fontVariantNumeric: 'tabular-nums' }}>{value}</span>
    </div>
  )
}
