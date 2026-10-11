import { usePolling } from '../hooks/useApi'

export default function Settings() {
  const { data: health } = usePolling('/health', 10000)
  const { data: hdfs } = usePolling('/hdfs/status', 10000)

  return (
    <div className="workspace">
      <div className="workspace-body">
        <div className="panel" style={{ maxWidth: 560 }}>
          <div className="panel-header">
            <span className="panel-title">Laboratory Settings</span>
          </div>
          <div className="panel-body" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-5)' }}>
            {/* HDFS Status */}
            <div>
              <h3 style={{ fontSize: 'var(--text-md)', fontWeight: 600, marginBottom: 'var(--sp-2)' }}>
                HDFS Dataset Status
              </h3>
              <p style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', marginBottom: 'var(--sp-3)' }}>
                The ClarkNet access_log is automatically loaded into HDFS during application startup and
                persists across restarts via Docker volumes.
              </p>
              <div style={{ display: 'flex', gap: 'var(--sp-2)', alignItems: 'center', flexWrap: 'wrap' }}>
                <div style={{ display: 'flex', gap: 'var(--sp-2)', alignItems: 'center' }}>
                  <span className={`dot ${hdfs?.dataset_ready ? 'green' : 'amber'}`} />
                  <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)' }}>
                    HDFS: {hdfs?.dataset_ready ? 'Dataset Ready' : 'Waiting for HDFS…'}
                  </span>
                </div>
                <div style={{ display: 'flex', gap: 'var(--sp-2)', alignItems: 'center' }}>
                  <span className={`dot ${health?.dataset ? 'green' : 'red'}`} />
                  <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)' }}>
                    Local: {health?.dataset ? 'Present' : 'Missing'}
                  </span>
                </div>
              </div>
              <div style={{
                marginTop: 'var(--sp-2)', padding: 'var(--sp-2) var(--sp-3)',
                background: 'rgba(56, 189, 248, 0.06)',
                border: '1px solid rgba(56, 189, 248, 0.15)',
                borderRadius: 'var(--radius-sm)', fontSize: 'var(--text-xs)',
                color: 'var(--text-muted)', fontFamily: 'var(--font-mono)',
              }}>
                /user/data/raw_logs/access_log
              </div>
            </div>

            {/* API Info */}
            <div style={{ borderTop: '1px solid var(--border-default)', paddingTop: 'var(--sp-4)' }}>
              <h3 style={{ fontSize: 'var(--text-md)', fontWeight: 600, marginBottom: 'var(--sp-2)' }}>
                API Configuration
              </h3>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-2)' }}>
                <InfoRow label="Backend URL" value="/api (proxied)" />
                <InfoRow label="WebSocket" value="/ws/stream" />
                <InfoRow label="HDFS NameNode" value="namenode:9870" />
                <InfoRow label="MongoDB" value="localhost:27018" />
                <InfoRow label="Spark Master" value="spark-master:7077" />
                <InfoRow label="YARN RM" value="resourcemanager:8088" />
              </div>
            </div>

            {/* About */}
            <div style={{ borderTop: '1px solid var(--border-default)', paddingTop: 'var(--sp-4)' }}>
              <h3 style={{ fontSize: 'var(--text-md)', fontWeight: 600, marginBottom: 'var(--sp-2)' }}>
                About
              </h3>
              <p style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', lineHeight: 1.6 }}>
                ClarkNet Big Data Analytics Laboratory. Built for distributed computing course work.
                This system demonstrates HDFS storage, Hadoop MapReduce batch analytics, and
                Spark-based stream processing using the ClarkNet HTTP access log dataset (1995).
              </p>
              <div style={{ marginTop: 'var(--sp-2)', display: 'flex', gap: 'var(--sp-2)', flexWrap: 'wrap' }}>
                <span className="tag tag-teal">HDFS</span>
                <span className="tag tag-blue">MapReduce</span>
                <span className="tag tag-violet">Spark</span>
                <span className="tag tag-amber">MongoDB</span>
                <span className="tag tag-green">FastAPI</span>
                <span className="tag tag-blue">React</span>
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
      <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>{value}</span>
    </div>
  )
}
