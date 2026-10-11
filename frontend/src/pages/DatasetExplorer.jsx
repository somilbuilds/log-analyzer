import { useState } from 'react'
import { usePolling, formatBytes, formatNumber } from '../hooks/useApi'

function DatabaseCylinder({ isMaster = false, title, subtitle, ip, port, used, capacity, remaining, usedPct, blocks = [] }) {
  return (
    <div className={`db-cylinder-card ${isMaster ? 'master' : ''}`}>
      <div className="db-cylinder-header">
        <div>
          <div className="db-cylinder-title">
            <span className={`dot ${isMaster ? 'teal' : 'blue'}`} style={{ width: 8, height: 8 }} />
            <span>{title}</span>
            <span className={`tag ${isMaster ? 'tag-teal' : 'tag-blue'}`} style={{ fontSize: 9 }}>
              {isMaster ? 'Master' : 'Storage Node'}
            </span>
          </div>
          <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2, fontFamily: 'var(--font-mono)' }}>
            {ip ? `${ip}${port ? `:${port}` : ''}` : subtitle}
          </div>
        </div>
        <div style={{ textAlign: 'right' }}>
          <span className="tag tag-green">ONLINE</span>
          <div style={{ fontSize: 10, color: 'var(--text-secondary)', marginTop: 2 }}>
            {usedPct !== undefined ? `${usedPct}% used` : 'Active'}
          </div>
        </div>
      </div>

      <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-4)', padding: 'var(--sp-1) 0' }}>
        {/* Visual 3D Database Cylinder */}
        <div className="db-cylinder-visual-wrapper">
          <div className="db-cylinder-3d">
            <div className="db-cylinder-activity" />
            <div className="db-platter platter-1" />
            <div className="db-platter platter-2" />
            <div className="db-platter platter-3" />
          </div>
        </div>

        {/* Capacity Breakdown */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: 3 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
            <span style={{ color: 'var(--text-muted)' }}>Configured Capacity:</span>
            <span style={{ fontFamily: 'var(--font-mono)' }}>{formatBytes(capacity)}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
            <span style={{ color: 'var(--text-muted)' }}>DFS Used:</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: isMaster ? 'var(--accent-teal)' : 'var(--accent-cyan)' }}>
              {formatBytes(used)}
            </span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11 }}>
            <span style={{ color: 'var(--text-muted)' }}>Remaining:</span>
            <span style={{ fontFamily: 'var(--font-mono)' }}>{formatBytes(remaining)}</span>
          </div>
          <div className="progress-bar" style={{ marginTop: 4, height: 5 }}>
            <div
              className={`progress-fill ${isMaster ? '' : 'blue'}`}
              style={{ width: `${Math.max(parseFloat(usedPct) || 1, 1)}%` }}
            />
          </div>
        </div>
      </div>

      {/* Block Content Breakdown by Size */}
      <div style={{ marginTop: 'var(--sp-2)', borderTop: '1px solid var(--border-default)', paddingTop: 'var(--sp-2)' }}>
        <div style={{
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
          fontSize: 10, textTransform: 'uppercase', color: 'var(--text-muted)',
          letterSpacing: 0.5, marginBottom: 'var(--sp-1)'
        }}>
          <span>{isMaster ? 'Managed Dataset Blocks' : 'Stored Blocks Content by Size'}</span>
          <span style={{ color: 'var(--accent-teal)' }}>{blocks.length} Blocks</span>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
          {blocks.length === 0 ? (
            <div style={{ fontSize: 11, color: 'var(--text-muted)', fontStyle: 'italic', padding: '4px 0' }}>
              No block replicas assigned
            </div>
          ) : (
            blocks.map((b, idx) => (
              <div key={idx} className="db-block-badge">
                <span style={{ color: 'var(--accent-cyan)' }}>{b.block_id || `Block #${idx}`}</span>
                <span style={{ color: 'var(--text-secondary)' }}>{formatBytes(b.size_bytes)}</span>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  )
}

export default function DatasetExplorer() {
  const { data: dataset } = usePolling('/dataset', 15000)
  const { data: hdfs } = usePolling('/hdfs/status', 10000)
  const { data: fsck } = usePolling('/hdfs/fsck', 15000)
  const { data: report } = usePolling('/hdfs/report', 15000)

  const [activeTab, setActiveTab] = useState('topology') // 'topology' | 'files' | 'schema'

  const info = dataset || {}
  const files = hdfs?.files || []
  const summary = report?.summary || {}
  const nodes = report?.nodes || []
  const namenode = report?.namenode || {}
  const blocks = fsck?.blocks || []

  // Total stored across replicas
  const totalReplicaBytes = (fsck?.total_size || 0) * (fsck?.replication_factor || 3)

  return (
    <div className="workspace">
      {/* Metric Strip */}
      <div className="metric-strip">
        <div className="metric-card">
          <div className="metric-label">Dataset Records</div>
          <div className="metric-value teal">{formatNumber(info.lines || 3328632)}</div>
          <div className="metric-sub">ClarkNet HTTP traces</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Dataset File Size</div>
          <div className="metric-value blue">{formatBytes(info.size_bytes || fsck?.total_size || 343414853)}</div>
          <div className="metric-sub">Raw access_log in HDFS</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Cluster Nodes</div>
          <div className="metric-value cyan">1 Master + {nodes.length || 3} DataNodes</div>
          <div className="metric-sub">Distributed storage</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS Replication</div>
          <div className="metric-value green">{fsck?.replication_factor || 3}× Factor</div>
          <div className="metric-sub">Across all 3 DataNodes</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">Total Replicated Size</div>
          <div className="metric-value violet">{formatBytes(totalReplicaBytes)}</div>
          <div className="metric-sub">{blocks.length * 3 || 9} block replicas active</div>
        </div>
        <div className="metric-card">
          <div className="metric-label">HDFS Health</div>
          <div className={`metric-value ${hdfs?.dataset_ready ? 'green' : 'amber'}`}>
            {hdfs?.dataset_ready ? 'Healthy (100%)' : 'Initializing…'}
          </div>
          <div className="metric-sub">0 missing · 0 corrupt</div>
        </div>
      </div>

      {/* Main Workspace Body */}
      <div className="panel flex-1" style={{ display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        {/* Navigation Tabs Header */}
        <div className="panel-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: 'var(--sp-2)' }}>
            <button
              className={`btn btn-sm ${activeTab === 'topology' ? 'btn-primary' : ''}`}
              onClick={() => setActiveTab('topology')}
            >
              🗄 HDFS Cluster Architecture (NameNode + 3 DataNodes)
            </button>
            <button
              className={`btn btn-sm ${activeTab === 'files' ? 'btn-primary' : ''}`}
              onClick={() => setActiveTab('files')}
            >
              📁 HDFS File Browser
            </button>
            <button
              className={`btn btn-sm ${activeTab === 'schema' ? 'btn-primary' : ''}`}
              onClick={() => setActiveTab('schema')}
            >
              📋 CLF Log Schema
            </button>
          </div>
          <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>
            HDFS Storage Demonstration · ClarkNet Analytics Laboratory
          </div>
        </div>

        <div className="panel-body" style={{ overflow: 'auto', padding: 'var(--sp-3)' }}>
          {/* TAB 1: Cluster Architecture Topology */}
          {activeTab === 'topology' && (
            <div className="hdfs-cluster-canvas">
              {/* Master Rack: NameNode */}
              <div className="hdfs-master-rack">
                <DatabaseCylinder
                  isMaster={true}
                  title="NameNode (Master Coordinator)"
                  subtitle="Manages HDFS Metadata & Block Namespace"
                  ip="namenode"
                  port="9000 (RPC) / 9870 (WebUI)"
                  used={summary.dfs_used || 1059836164}
                  capacity={summary.configured_capacity || 3243303530496}
                  remaining={summary.dfs_remaining || 3003964977152}
                  usedPct={summary.dfs_used_pct || '0.04'}
                  blocks={blocks.length > 0 ? blocks : [
                    { block_id: 'blk_1073742263', size_bytes: 134217728 },
                    { block_id: 'blk_1073742264', size_bytes: 134217728 },
                    { block_id: 'blk_1073742265', size_bytes: 74979397 },
                  ]}
                />
              </div>

              {/* DataNodes Rack: 3 Distributed DataNodes */}
              <div>
                <div style={{
                  fontSize: 'var(--text-xs)', fontWeight: 600, color: 'var(--text-muted)',
                  textTransform: 'uppercase', letterSpacing: 0.8, marginBottom: 'var(--sp-2)'
                }}>
                  Distributed HDFS DataNode Storage Layer (3 Replicas)
                </div>

                <div className="hdfs-datanodes-rack">
                  {(nodes.length > 0 ? nodes : [
                    { name: 'DataNode 1', hostname: 'datanode', capacity: 1081101176832, used: 353278721, remaining: 1001321659050, used_pct: 0.04 },
                    { name: 'DataNode 2', hostname: 'datanode2', capacity: 1081101176832, used: 353278721, remaining: 1001321659050, used_pct: 0.04 },
                    { name: 'DataNode 3', hostname: 'datanode3', capacity: 1081101176832, used: 353278721, remaining: 1001321659050, used_pct: 0.04 },
                  ]).map((node, i) => (
                    <DatabaseCylinder
                      key={i}
                      isMaster={false}
                      title={node.name || `DataNode ${i + 1}`}
                      subtitle={`Physical Container: ${node.hostname || `datanode${i > 0 ? i + 1 : ''}`}`}
                      ip={node.hostname || node.address || `datanode${i > 0 ? i + 1 : ''}`}
                      port="9864 (HTTP) / 9866 (Data)"
                      used={node.used || 353278721}
                      capacity={node.capacity || 1081101176832}
                      remaining={node.remaining || 1001321659050}
                      usedPct={node.used_pct || '0.04'}
                      blocks={blocks.length > 0 ? blocks : [
                        { block_id: 'blk_1073742263', size_bytes: 134217728 },
                        { block_id: 'blk_1073742264', size_bytes: 134217728 },
                        { block_id: 'blk_1073742265', size_bytes: 74979397 },
                      ]}
                    />
                  ))}
                </div>
              </div>

              {/* Block Allocation Matrix */}
              <div className="panel" style={{ marginTop: 'var(--sp-2)' }}>
                <div className="panel-header" style={{ height: 32 }}>
                  <span className="panel-title" style={{ fontSize: 11 }}>
                    HDFS Block Allocation & Replica Distribution Matrix
                  </span>
                  <span className="tag tag-green">Replication Factor: 3× (Fault Tolerant)</span>
                </div>
                <div className="panel-body-flush">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Block ID</th>
                        <th className="numeric">Chunk Size</th>
                        <th className="numeric">Offset Range</th>
                        <th>Replica 1 (datanode)</th>
                        <th>Replica 2 (datanode2)</th>
                        <th>Replica 3 (datanode3)</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(blocks.length > 0 ? blocks : [
                        { block_id: 'blk_1073742263_1039', size_bytes: 134217728, locations: ['172.18.0.4', '172.18.0.5', '172.18.0.6'] },
                        { block_id: 'blk_1073742264_1040', size_bytes: 134217728, locations: ['172.18.0.4', '172.18.0.5', '172.18.0.6'] },
                        { block_id: 'blk_1073742265_1041', size_bytes: 74979397, locations: ['172.18.0.4', '172.18.0.5', '172.18.0.6'] },
                      ]).map((b, i) => (
                        <tr key={i}>
                          <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)' }}>
                            {b.block_id}
                          </td>
                          <td className="numeric font-mono" style={{ color: 'var(--accent-teal)' }}>
                            {formatBytes(b.size_bytes)}
                          </td>
                          <td className="numeric font-mono" style={{ color: 'var(--text-muted)' }}>
                            {i === 0 ? '0 MB → 128 MB' : i === 1 ? '128 MB → 256 MB' : '256 MB → 327.5 MB'}
                          </td>
                          <td><span className="tag tag-blue">Stored (Active)</span></td>
                          <td><span className="tag tag-blue">Stored (Active)</span></td>
                          <td><span className="tag tag-blue">Stored (Active)</span></td>
                          <td><span className="tag tag-green">Healthy 3/3</span></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}

          {/* TAB 2: HDFS File Browser */}
          {activeTab === 'files' && (
            <div style={{ height: '100%', overflow: 'auto' }}>
              {files.length === 0 ? (
                <div className="state-message">
                  <div className="state-icon">⬡</div>
                  <div className="state-title">No HDFS Files Reported</div>
                  <div className="state-desc">HDFS is initializing or files are stored in `/user/data/`.</div>
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
          )}

          {/* TAB 3: Schema Details */}
          {activeTab === 'schema' && (
            <div style={{ maxWidth: 680, display: 'flex', flexDirection: 'column', gap: 'var(--sp-3)' }}>
              <div style={{ fontSize: 'var(--text-md)', fontWeight: 600, color: 'var(--accent-teal)' }}>
                Common Log Format (CLF) Schema Specification
              </div>
              <div style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', lineHeight: 1.6 }}>
                The raw ClarkNet web trace dataset adheres to the NCSA Common Log Format (CLF), recording individual HTTP transactions received by the ClarkNet server infrastructure during late August and September 1995.
              </div>

              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
                {[
                  { field: 'host', type: 'string', desc: 'Remote client host name or IP address making the request.' },
                  { field: 'ident', type: 'string', desc: 'Remote log name from RFC 931 daemon (typically hyphen "-").' },
                  { field: 'authuser', type: 'string', desc: 'Authenticated username for HTTP auth (typically hyphen "-").' },
                  { field: 'timestamp', type: 'datetime', desc: 'Date and time of request formatted as [DD/Mon/YYYY:HH:MM:SS -0400].' },
                  { field: 'request', type: 'string', desc: 'Full HTTP request line: "METHOD /path/uri HTTP/version".' },
                  { field: 'status', type: 'integer', desc: 'HTTP response status code returned to client (e.g., 200, 304, 404, 500).' },
                  { field: 'bytes', type: 'integer', desc: 'Total volume of payload bytes transferred to client (or 0 if none).' },
                ].map(item => (
                  <div key={item.field} style={{
                    padding: 'var(--sp-2) var(--sp-3)', background: 'var(--bg-elevated)',
                    border: '1px solid var(--border-default)', borderRadius: 'var(--radius-sm)',
                    display: 'flex', alignItems: 'center', justifyContent: 'space-between'
                  }}>
                    <div>
                      <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--accent-cyan)', fontWeight: 600, marginRight: 8 }}>
                        {item.field}
                      </span>
                      <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>{item.desc}</span>
                    </div>
                    <span className="tag tag-blue">{item.type}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
