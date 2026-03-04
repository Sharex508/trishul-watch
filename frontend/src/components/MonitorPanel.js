import React, { useEffect, useState } from 'react';
import '../App.css';

const API_URL = process.env.REACT_APP_API_URL || '';

export default function MonitorPanel() {
  const [lines, setLines] = useState([]);
  const [filter, setFilter] = useState('');
  const [logView, setLogView] = useState('errors'); // all | errors | warnings
  const [health, setHealth] = useState({});
  const [tab, setTab] = useState('api'); // future: api|ingestor|pattern

  const loadLogs = async () => {
    try {
      const effectiveFilter = filter || (logView === 'errors' ? 'ERROR' : logView === 'warnings' ? 'WARNING' : '');
      const res = await fetch(`${API_URL}/api/logs/recent?lines=200${effectiveFilter ? `&filter_text=${encodeURIComponent(effectiveFilter)}` : ''}`);
      const json = await res.json();
      setLines(Array.isArray(json) ? json : []);
    } catch (e) {
      setLines([`Error loading logs: ${e.message || e}`]);
    }
  };

  const loadHealth = async () => {
    try {
      const res = await fetch(`${API_URL}/api/health/summary`);
      const json = await res.json();
      setHealth(json || {});
    } catch (e) {
      setHealth({ error: e.message || String(e) });
    }
  };

  useEffect(() => {
    loadLogs();
    loadHealth();
    const id = setInterval(loadLogs, 5000);
    const id2 = setInterval(loadHealth, 5000);
    return () => { clearInterval(id); clearInterval(id2); };
  }, [filter, logView]);

  return (
    <div style={{ display: 'grid', gap: 16, width: '100%', gridTemplateColumns: '1fr 1fr' }}>
      <div className="overlay-card" style={{ minHeight: 200 }}>
        <h3 style={{ marginTop: 0 }}>Health</h3>
        <div style={{ display: 'grid', gap: 8, fontSize: 13, textAlign: 'left' }}>
          <div>Trading: <b style={{ color: health.trading_enabled ? '#22c55e' : '#f59e0b' }}>{health.trading_enabled ? 'ENABLED' : 'DISABLED'}</b> · mode: {health.strategy_mode || '-'}</div>
          <div>Coin Brain: {health.coin_brain_symbol || '-'}</div>
          <div>Trade logs: {health.trade_logs ?? '-'} · Zones: {health.zones ?? '-'} · Plans: {health.entry_plans ?? '-'}</div>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }}>
            <span className="tag">Latest candles:</span>
            {health.latest_candles && Object.entries(health.latest_candles).map(([sym, ts]) => (
              <span key={sym} className="tag">{sym}: {ts}</span>
            ))}
          </div>
          {health.error && <div style={{ color: '#ef4444' }}>Health error: {health.error}</div>}
        </div>
      </div>
      <div className="overlay-card" style={{ minHeight: 200 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h3 style={{ marginTop: 0 }}>Logs</h3>
          <div style={{ display: 'flex', gap: 8 }}>
            <select className="overlay-select" value={tab} onChange={(e) => setTab(e.target.value)}>
              <option value="api">API</option>
            </select>
            <select className="overlay-select" value={logView} onChange={(e) => setLogView(e.target.value)}>
              <option value="errors">Errors</option>
              <option value="warnings">Warnings</option>
              <option value="all">All</option>
            </select>
            <input
              className="overlay-input"
              placeholder="Filter text (optional)"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ width: 160 }}
            />
          </div>
        </div>
        <div style={{ maxHeight: 320, overflowY: 'auto', textAlign: 'left', fontFamily: 'monospace', fontSize: 12, background: '#0f172a', border: '1px solid #1f2937', borderRadius: 8, padding: 8 }}>
          {lines.map((ln, idx) => (
            <div key={idx} style={{ whiteSpace: 'pre-wrap' }}>{ln}</div>
          ))}
          {lines.length === 0 && <div style={{ color: '#9ca3af' }}>No logs available.</div>}
        </div>
      </div>
    </div>
  );
}
