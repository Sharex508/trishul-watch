import React, { useEffect, useState } from 'react';
import './AdminPanel.css';

const API_URL = process.env.REACT_APP_API_URL || '';

export default function TradingPanel() {
  const [status, setStatus] = useState({ enabled: false });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');

  const loadStatus = async () => {
    try {
      const res = await fetch(`${API_URL}/api/trading/status`);
      const json = await res.json();
      setStatus(json);
    } catch (e) {
      // ignore transient
    }
  };

  useEffect(() => {
    loadStatus();
    const id = setInterval(loadStatus, 5000);
    return () => clearInterval(id);
  }, []);

  const call = async (path) => {
    setBusy(true); setError(''); setMessage('');
    try {
      const res = await fetch(`${API_URL}/api/trading/${path}`, { method: 'POST' });
      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || `HTTP ${res.status}`);
      }
      const json = await res.json();
      setMessage(`${path.toUpperCase()} ok`);
      if (json.enabled !== undefined) setStatus(json);
      if (path === 'reset') setMessage('All trade logs cleared.');
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="admin-panel">
      <h2>Trading Controls</h2>
      <p>
        Status: {status.enabled ? (
          <span style={{ color: '#22c55e' }}>ENABLED</span>
        ) : (
          <span style={{ color: '#f59e0b' }}>DISABLED</span>
        )}
      </p>
      <div style={{ display: 'flex', gap: '10px', margin: '10px 0' }}>
        <button disabled={busy} onClick={() => call('start')} className="admin-button">Start</button>
        <button disabled={busy} onClick={() => call('stop')} className="admin-button">Stop</button>
        <button disabled={busy} onClick={() => call('reset')} className="admin-button">Reset</button>
      </div>
      <p style={{ fontSize: 12, color: '#9ca3af' }}>
        Start begins a simple paper-trading loop on live prices; Stop halts it; Reset clears all completed trade entries.
      </p>
      {message && <div style={{ color: '#22c55e', marginTop: 8 }}>{message}</div>}
      {error && <div style={{ color: '#ef4444', marginTop: 8 }}>Error: {error}</div>}
    </div>
  );
}
