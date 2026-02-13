import React, { useEffect, useMemo, useState } from 'react';
import './AdminPanel.css';

const API_URL = process.env.REACT_APP_API_URL || '';

export default function PredictionsPanel() {
  const [timeframe, setTimeframe] = useState('1m');
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = async () => {
    setError('');
    try {
      const res = await fetch(`${API_URL}/api/predictions?timeframe=${encodeURIComponent(timeframe)}&limit=200`);
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setRows(Array.isArray(data) ? data : []);
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    setLoading(true);
    load();
    const id = setInterval(load, 25000);
    return () => clearInterval(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [timeframe]);

  const hasData = useMemo(() => rows && rows.length > 0, [rows]);

  return (
    <div className="admin-panel">
      <h2>Predictions</h2>
      <p style={{ fontSize: 12, color: '#9ca3af' }}>
        Ranked list of symbols likely to show bullish momentum based on the latest discovered patterns. 
        Scores are heuristic (0–100%) and consider cluster average return, volatility, win‑rate, and sample size.
      </p>

      <div style={{ display: 'flex', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
        <div>
          <label style={{ fontSize: 12, color: '#9ca3af' }}>Timeframe</label><br />
          <select value={timeframe} onChange={(e) => setTimeframe(e.target.value)} className="admin-input">
            <option value="1m">1m</option>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
          </select>
        </div>
        <button className="admin-button" onClick={load}>Refresh</button>
      </div>

      {loading && <div>Loading predictions…</div>}
      {error && <div style={{ color: '#ef4444' }}>Error: {error}</div>}

      {!loading && !hasData && (
        <div style={{ color: '#9ca3af' }}>
          No predictions yet. Make sure patterns exist (run discovery) and give the ingestors a minute.
        </div>
      )}

      {hasData && (
        <div style={{ overflowX: 'auto' }}>
          <table className="admin-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Symbol</th>
                <th>Score %</th>
                <th>Regime</th>
                <th>Pattern ID</th>
                <th>Avg Ret</th>
                <th>Volatility</th>
                <th>Win‑rate</th>
                <th>Cluster Size</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r, idx) => (
                <tr key={`${r.symbol}-${r.pattern_id}-${idx}`}>
                  <td>{idx + 1}</td>
                  <td>{r.symbol}</td>
                  <td style={{ fontWeight: 600 }}>{Number(r.score_pct || 0).toFixed(2)}</td>
                  <td>{r.regime || '—'}</td>
                  <td>{r.pattern_id || '—'}</td>
                  <td>{r.avg_return != null ? Number(r.avg_return).toFixed(5) : ''}</td>
                  <td>{r.volatility != null ? Number(r.volatility).toFixed(5) : ''}</td>
                  <td>{r.win_rate != null ? (Number(r.win_rate) * 100).toFixed(1) + '%' : ''}</td>
                  <td>{r.cluster_size != null ? r.cluster_size : ''}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
