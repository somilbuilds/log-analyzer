/**
 * Custom hooks for API communication and WebSocket connections.
 */
import { useState, useEffect, useRef, useCallback } from 'react'

const API_BASE = '/api'

/** Generic fetch with JSON parsing and error handling. */
export async function apiFetch(path, opts = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...opts.headers },
    ...opts,
  })
  if (!res.ok) throw new Error(`API ${res.status}: ${res.statusText}`)
  return res.json()
}

/** POST helper. */
export async function apiPost(path, params = {}) {
  const url = new URL(`${API_BASE}${path}`, window.location.origin)
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null) url.searchParams.set(k, v)
  })
  const res = await fetch(url.toString(), { method: 'POST' })
  return res.json()
}

/** Hook: poll an API endpoint at a given interval. */
export function usePolling(path, intervalMs = 5000, enabled = true) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(true)
  const mountedRef = useRef(true)

  const fetchData = useCallback(async () => {
    try {
      const result = await apiFetch(path)
      if (mountedRef.current) {
        setData(result)
        setError(null)
        setLoading(false)
      }
    } catch (err) {
      if (mountedRef.current) {
        setError(err.message)
        setLoading(false)
      }
    }
  }, [path])

  useEffect(() => {
    mountedRef.current = true
    if (!enabled) return
    fetchData()
    const id = setInterval(fetchData, intervalMs)
    return () => {
      mountedRef.current = false
      clearInterval(id)
    }
  }, [fetchData, intervalMs, enabled])

  return { data, error, loading, refetch: fetchData }
}

/** Hook: WebSocket connection with auto-reconnect. */
export function useWebSocket(path, options = {}) {
  const { onMessage, enabled = true, reconnectMs = 3000 } = options
  const wsRef = useRef(null)
  const reconnectTimer = useRef(null)
  const [connected, setConnected] = useState(false)

  const connect = useCallback(() => {
    if (!enabled) return
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const url = `${protocol}//${window.location.host}${path}`
    try {
      const ws = new WebSocket(url)
      ws.onopen = () => setConnected(true)
      ws.onclose = () => {
        setConnected(false)
        reconnectTimer.current = setTimeout(connect, reconnectMs)
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data)
          onMessage?.(data)
        } catch { /* ignore non-JSON */ }
      }
      wsRef.current = ws
    } catch {
      reconnectTimer.current = setTimeout(connect, reconnectMs)
    }
  }, [path, enabled, reconnectMs, onMessage])

  useEffect(() => {
    connect()
    return () => {
      clearTimeout(reconnectTimer.current)
      wsRef.current?.close()
    }
  }, [connect])

  return { connected, ws: wsRef.current }
}

/** Format bytes to human readable. */
export function formatBytes(bytes, decimals = 1) {
  if (!bytes || bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + ' ' + sizes[i]
}

/** Format large numbers with K/M suffixes. */
export function formatNumber(num) {
  if (num === null || num === undefined) return '—'
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M'
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K'
  return num.toLocaleString()
}

/** Format percentage. */
export function formatPct(val, decimals = 1) {
  if (val === null || val === undefined) return '—'
  return (val * 100).toFixed(decimals) + '%'
}

/** Get status class for HTTP status code. */
export function statusClass(code) {
  if (code >= 500) return 's5xx'
  if (code >= 400) return 's4xx'
  if (code >= 300) return 's3xx'
  if (code >= 200) return 's2xx'
  return ''
}

/** Chart color palette. */
export const CHART_COLORS = [
  '#3b82f6', '#2dd4bf', '#8b5cf6', '#f59e0b',
  '#22d3ee', '#ec4899', '#6366f1', '#14b8a6',
  '#f97316', '#a855f7', '#06b6d4', '#10b981'
]
