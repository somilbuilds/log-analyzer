import { usePolling } from '../hooks/useApi'

export default function JobHistory() {
  const { data, loading } = usePolling('/jobs?limit=100', 5000)
  const jobs = data?.jobs || []

  return (
    <div className="workspace">
      <div className="panel" style={{ flex: 1 }}>
        <div className="panel-header">
          <span className="panel-title">Job & Session History</span>
          <span style={{ fontSize: 'var(--text-xs)', color: 'var(--text-muted)' }}>{jobs.length} records</span>
        </div>
        <div className="panel-body-flush" style={{ overflow: 'auto' }}>
          {loading ? (
            <div className="state-message"><div className="spinner" /></div>
          ) : jobs.length === 0 ? (
            <div className="state-message">
              <div className="state-icon">☰</div>
              <div className="state-title">No Job History</div>
              <div className="state-desc">Run a MapReduce analysis or streaming session to create history entries.</div>
            </div>
          ) : (
            <table className="data-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Started</th>
                  <th>Duration</th>
                  <th>Input</th>
                  <th>Output</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((job, i) => (
                  <tr key={i}>
                    <td style={{ fontWeight: 500 }}>{job.task_name || job.task}</td>
                    <td>
                      <span className={`tag ${job.type === 'mapreduce' ? 'tag-blue' : 'tag-violet'}`}>
                        {job.type}
                      </span>
                    </td>
                    <td>
                      <span className={`job-status ${job.status}`}>
                        <span className={`dot ${job.status === 'completed' ? 'green' : job.status === 'running' ? 'blue' : job.status === 'failed' ? 'red' : ''}`} />
                        {job.status}
                      </span>
                    </td>
                    <td style={{ color: 'var(--text-secondary)', fontSize: '11px', fontFamily: 'var(--font-mono)' }}>
                      {job.start_time?.slice(0, 19)?.replace('T', ' ')}
                    </td>
                    <td className="numeric">{job.duration ? `${job.duration}s` : '—'}</td>
                    <td className="numeric">{job.input_records?.toLocaleString() ?? '—'}</td>
                    <td className="numeric">{job.output_records?.toLocaleString() ?? '—'}</td>
                    <td style={{ color: 'var(--color-danger)', fontSize: '10px', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {job.error || '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  )
}
