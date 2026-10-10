import { useState } from 'react'
import { apiPost, usePolling } from '../hooks/useApi'

export default function Settings() {
  const { data: health } = usePolling('/health', 10000)
  const [uploadStatus, setUploadStatus] = useState(null)
  const [uploading, setUploading] = useState(false)

  const uploadToHdfs = async () => {
    setUploading(true)
    setUploadStatus(null)
    try {
      const res = await apiPost('/hdfs/upload')
      setUploadStatus(res)
    } catch (err) {
      setUploadStatus({ available: false, message: err.message })
    }
    setUploading(false)
  }

  return (
    <div className="workspace">
      <div className="workspace-body">
        <div className="panel" style={{ maxWidth: 560 }}>
          <div className="panel-header">
            <span className="panel-title">Laboratory Settings</span>
          </div>
          <div className="panel-body" style={{ display: 'flex', flexDirection: 'column', gap: 'var(--sp-5)' }}>
            {/* HDFS Upload */}
            <div>
              <h3 style={{ fontSize: 'var(--text-md)', fontWeight: 600, marginBottom: 'var(--sp-2)' }}>
                HDFS Dataset Management
              </h3>
              <p style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)', marginBottom: 'var(--sp-3)' }}>
                Upload or re-upload the ClarkNet access_log to HDFS. This copies the local dataset
                into HDFS at <code style={{ color: 'var(--accent-cyan)', fontFamily: 'var(--font-mono)' }}>/user/data/raw_logs/access_log</code>.
              </p>
              <div style={{ display: 'flex', gap: 'var(--sp-2)', alignItems: 'center' }}>
                <button className="btn btn-primary" onClick={uploadToHdfs} disabled={uploading}>
                  {uploading ? '⏳ Uploading…' : '↑ Upload to HDFS'}
                </button>
                <span className={`dot ${health?.dataset ? 'green' : 'red'}`} />
                <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-secondary)' }}>
                  Local dataset: {health?.dataset ? 'Present' : 'Missing'}
                </span>
              </div>
              {uploadStatus && (
                <div style={{
                  marginTop: 'var(--sp-2)', padding: 'var(--sp-2) var(--sp-3)',
                  background: uploadStatus.available ? 'rgba(34, 197, 94, 0.06)' : 'rgba(239, 68, 68, 0.06)',
                  border: `1px solid ${uploadStatus.available ? 'rgba(34, 197, 94, 0.2)' : 'rgba(239, 68, 68, 0.2)'}`,
                  borderRadius: 'var(--radius-sm)', fontSize: 'var(--text-xs)',
                  color: uploadStatus.available ? 'var(--color-success)' : 'var(--color-danger)',
                }}>
                  {uploadStatus.available ? '✓ Dataset uploaded successfully' : `✗ ${uploadStatus.message}`}
                </div>
              )}
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
