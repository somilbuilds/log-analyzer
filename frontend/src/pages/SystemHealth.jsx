import { usePolling, formatBytes } from '../hooks/useApi'

export default function SystemHealth() {
  const { data: health } = usePolling('/health', 5000)
  const { data: report } = usePolling('/hdfs/report', 10000)
  const { data: status } = usePolling('/status', 5000)
  const { data: fsck } = usePolling('/hdfs/fsck', 15000)

  const h = health || {}
  const summary = report?.summary || {}
  const nodes = report?.nodes || []
  const st = status || {}
  const streamState = st.stream_state || {}

  const services = [
    { name: 'FastAPI Backend', ok: h.ok, port: 8000, desc: 'REST API + WebSocket server' },
    { name: 'HDFS NameNode', ok: h.hdfs, port: 9870, desc: 'Hadoop Distributed File System master' },
    { name: 'MongoDB', ok: h.mongo, port: 27018, desc: 'Document store for streaming aggregates' },
    { name: 'ClarkNet Dataset', ok: h.dataset, desc: 'Local access_log file present' },
    { name: 'YARN ResourceManager', ok: h.hdfs, port: 8088, desc: 'MapReduce job scheduler' },
    { name: 'Spark Master', ok: null, port: 8080, desc: 'Spark cluster manager' },
  ]

  return (
    <div className="workspace">
      <div className="workspace-body">
        {/* Service Status Panel */}
        <div className="panel flex-1">
          <div className="panel-header">
            <span className="panel-title">Service Health</span>
            <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)' }}>
              Last check: {h.time?.slice(11, 19) || '—'}
            </span>
          </div>
          <div className="panel-body">
            <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
              {services.map((s, i) => (
                <div key={i} style={{
                  display: 'flex', alignItems: 'center', gap: 'var(--sp-3)',
                  padding: 'var(--sp-2) var(--sp-3)',
                  background: 'var(--bg-elevated)', borderRadius: 'var(--radius-sm)',
                  border: '1px solid var(--border-default)'
                }}>
                  <span className={`dot ${s.ok === true ? 'green' : s.ok === false ? 'red' : 'amber'}`}
                        style={{ width: 8, height: 8 }} />
                  <div style={{ flex: 1 }}>
                    <div style={{ fontWeight: 500, fontSize: 'var(--text-sm)' }}>{s.name}</div>
                    <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{s.desc}</div>
                  </div>
                  {s.port && (
                    <span style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)' }}>
                      :{s.port}
                    </span>
                  )}
                  <span className={`tag ${s.ok === true ? 'tag-green' : s.ok === false ? 'tag-red' : 'tag-amber'}`}>
                    {s.ok === true ? 'Online' : s.ok === false ? 'Down' : 'Unknown'}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Right column */}
        <div className="flex-col gap-3" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)', width: 360, flexShrink: 0 }}>
          {/* HDFS Summary */}
          <div className="panel" style={{ flex: 1 }}>
            <div className="panel-header">
              <span className="panel-title">HDFS Cluster</span>
            </div>
            <div className="panel-body">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
                <InfoRow label="Configured Capacity" value={formatBytes(summary.configured_capacity)} />
                <InfoRow label="Present Capacity" value={formatBytes(summary.present_capacity)} />
                <InfoRow label="DFS Used" value={formatBytes(summary.dfs_used)} />
                <InfoRow label="DFS Remaining" value={formatBytes(summary.dfs_remaining)} />
                <InfoRow label="Used %" value={summary.dfs_used_pct ? summary.dfs_used_pct + '%' : '—'} />
                <InfoRow label="Live DataNodes" value={summary.live_datanodes ?? '—'} />
                <InfoRow label="Dead DataNodes" value={summary.dead_datanodes ?? 0} />

                {summary.configured_capacity > 0 && (
                  <div style={{ marginTop: 'var(--sp-2)' }}>
                    <div className="storage-bar">
                      <div className="storage-segment" style={{
                        width: `${summary.dfs_used_pct || 0}%`,
                        background: 'var(--accent-blue)'
                      }} />
                    </div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 4, fontSize: 10, color: 'var(--text-muted)' }}>
                      <span>Used: {formatBytes(summary.dfs_used)}</span>
                      <span>Free: {formatBytes(summary.dfs_remaining)}</span>
                    </div>
                  </div>
                )}

                <div style={{ borderTop: '1px solid var(--border-default)', paddingTop: 'var(--sp-2)', marginTop: 'var(--sp-1)' }}>
                  <div style={{ fontSize: 'var(--text-xs)', fontWeight: 600, color: 'var(--text-muted)', marginBottom: 'var(--sp-1)' }}>FSCK</div>
                  <InfoRow label="Total Files" value={fsck?.total_files ?? '—'} />
                  <InfoRow label="Total Blocks" value={fsck?.total_blocks ?? '—'} />
                  <InfoRow label="Min Replicated" value={fsck?.min_replicated ?? '—'} />
                  <InfoRow label="Under Replicated" value={fsck?.under_replicated ?? 0} />
                  <InfoRow label="Over Replicated" value={fsck?.over_replicated ?? 0} />
                </div>
              </div>
            </div>
          </div>

          {/* Stream Pipeline */}
          <div className="panel" style={{ flex: 0 }}>
            <div className="panel-header">
              <span className="panel-title">Stream Pipeline</span>
              <span className={`tag ${streamState.status === 'running' ? 'tag-green' : streamState.status === 'paused' ? 'tag-amber' : 'tag-red'}`}>
                {streamState.status || 'stopped'}
              </span>
            </div>
            <div className="panel-body">
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-1)' }}>
                <InfoRow label="Replayer" value={st.demo?.replayer ? 'Running' : 'Stopped'} />
                <InfoRow label="Stream Job" value={st.demo?.stream ? 'Running' : 'Stopped'} />
                <InfoRow label="Replay Speed" value={`${streamState.speed || 0} eps`} />
                <InfoRow label="Windows" value={st.window_count ?? 0} />
                <InfoRow label="Last Update" value={st.last_update?.slice(11, 19) || '—'} />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

function InfoRow({ label, value }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 'var(--text-xs)', lineHeight: 1.8 }}>
      <span style={{ color: 'var(--text-muted)' }}>{label}</span>
      <span style={{ fontVariantNumeric: 'tabular-nums', color: 'var(--text-primary)' }}>{value}</span>
    </div>
  )
}
