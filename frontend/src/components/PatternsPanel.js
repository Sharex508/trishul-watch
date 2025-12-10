import React, { useEffect, useMemo, useState } from 'react';
import './AdminPanel.css';

const API_URL = process.env.REACT_APP_API_URL || '';

export default function PatternsPanel() {
  const [symbols, setSymbols] = useState([]);
  const [symbol, setSymbol] = useState('');
  const [timeframe, setTimeframe] = useState('1m');
  const [candles, setCandles] = useState([]);
  const [features, setFeatures] = useState([]);
  const [patterns, setPatterns] = useState([]);
  const [active, setActive] = useState(null);
  const [regime, setRegime] = useState(null);
  const [error, setError] = useState('');

  // Load symbols from existing monitors
  useEffect(() => {
    const loadSymbols = async () => {
      try {
        const res = await fetch(`${API_URL}/api/coin-monitors`);
        const data = await res.json();
        const syms = data.map((c) => c.symbol);
        setSymbols(syms);
        if (syms.length && !symbol) setSymbol(syms[0]);
      } catch (e) {
        setError(e.message || String(e));
      }
    };
    loadSymbols();
  }, []);

  // Load patterns library periodically
  useEffect(() => {
    const loadPatterns = async () => {
      try {
        const res = await fetch(`${API_URL}/api/patterns?limit=200`);
        const data = await res.json();
        setPatterns(Array.isArray(data) ? data : []);
      } catch (e) {
        // ignore transient
      }
    };
    loadPatterns();
    const id = setInterval(loadPatterns, 30000);
    return () => clearInterval(id);
  }, []);

  // Load symbol-specific data
  useEffect(() => {
    if (!symbol) return;
    const loadAll = async () => {
      try {
        setError('');
        const [cRes, fRes, aRes, rRes] = await Promise.all([
          fetch(`${API_URL}/api/market/candles/latest?symbol=${symbol}&timeframe=${timeframe}&limit=120`),
          fetch(`${API_URL}/api/market/features/latest?symbol=${symbol}&timeframe=${timeframe}&limit=120`),
          fetch(`${API_URL}/api/patterns/active?symbol=${symbol}&timeframe=${timeframe}`),
          fetch(`${API_URL}/api/regime/current?symbol=${symbol}&timeframe=${timeframe}`),
        ]);
        const [c, f, a, r] = await Promise.all([
          cRes.json(), fRes.json(), aRes.json(), rRes.json()
        ]);
        setCandles(Array.isArray(c) ? c : []);
        setFeatures(Array.isArray(f) ? f : []);
        setActive(a && Object.keys(a).length ? a : null);
        setRegime(r && Object.keys(r).length ? r : null);
      } catch (e) {
        setError(e.message || String(e));
      }
    };
    loadAll();
    const id = setInterval(loadAll, 20000);
    return () => clearInterval(id);
  }, [symbol, timeframe]);

  const latestFeature = useMemo(() => features && features[0] ? features[0] : null, [features]);

  return (
    <div className="admin-panel">
      <h2>Patterns</h2>
      <p style={{ fontSize: 12, color: '#9ca3af' }}>
        Unsupervised pattern discovery and regime snapshots. Data updates every ~20–30s.
      </p>

      <div style={{ display: 'flex', gap: 12, marginBottom: 12, flexWrap: 'wrap' }}>
        <div>
          <label style={{ fontSize: 12, color: '#9ca3af' }}>Symbol</label><br />
          <select value={symbol} onChange={(e) => setSymbol(e.target.value)} className="admin-input">
            {symbols.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
        <div>
          <label style={{ fontSize: 12, color: '#9ca3af' }}>Timeframe</label><br />
          <select value={timeframe} onChange={(e) => setTimeframe(e.target.value)} className="admin-input">
            <option value="1m">1m</option>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h">1h</option>
          </select>
        </div>
      </div>

      {error && <div style={{ color: '#ef4444', marginTop: 8 }}>Error: {error}</div>}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr', gap: 16 }}>
        <section>
          <h3 style={{ margin: '8px 0' }}>Latest Features</h3>
          {!latestFeature && <div style={{ color: '#9ca3af' }}>No features yet — ingestion will populate after first minute.</div>}
          {latestFeature && (
            <table className="admin-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>EMA7</th>
                  <th>EMA25</th>
                  <th>EMA slope</th>
                  <th>Ret 1</th>
                  <th>Ret 5</th>
                  <th>Ret 15</th>
                </tr>
              </thead>
              <tbody>
                {features.map((f, idx) => (
                  <tr key={idx}>
                    <td>{f.ts ? new Date(f.ts).toLocaleTimeString() : ''}</td>
                    <td>{Number(f.ema7 || 0).toFixed(6)}</td>
                    <td>{Number(f.ema25 || 0).toFixed(6)}</td>
                    <td>{Number(f.ema_slope || 0).toFixed(8)}</td>
                    <td>{Number(f.ret_1 || 0).toFixed(5)}</td>
                    <td>{Number(f.ret_5 || 0).toFixed(5)}</td>
                    <td>{Number(f.ret_15 || 0).toFixed(5)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>

        <section>
          <h3 style={{ margin: '8px 0' }}>Recent Candles</h3>
          {(!candles || candles.length === 0) && <div style={{ color: '#9ca3af' }}>No candles yet — will appear after ingestion.</div>}
          {candles && candles.length > 0 && (
            <table className="admin-table">
              <thead>
                <tr>
                  <th>Time</th>
                  <th>Open</th>
                  <th>High</th>
                  <th>Low</th>
                  <th>Close</th>
                  <th>Volume</th>
                </tr>
              </thead>
              <tbody>
                {candles.map((c, idx) => (
                  <tr key={idx}>
                    <td>{c.ts ? new Date(c.ts).toLocaleTimeString() : ''}</td>
                    <td>{Number(c.open || 0).toFixed(8)}</td>
                    <td>{Number(c.high || 0).toFixed(8)}</td>
                    <td>{Number(c.low || 0).toFixed(8)}</td>
                    <td>{Number(c.close || 0).toFixed(8)}</td>
                    <td>{Number(c.volume || 0).toFixed(3)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>

        <section>
          <h3 style={{ margin: '8px 0' }}>Pattern Library</h3>
          {(patterns || []).length === 0 && <div style={{ color: '#9ca3af' }}>No patterns yet — discovery job not implemented in this phase.</div>}
          {(patterns || []).length > 0 && (
            <table className="admin-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Symbol</th>
                  <th>Timeframe</th>
                  <th>Algo</th>
                  <th>Size</th>
                  <th>Avg Return</th>
                  <th>Volatility</th>
                  <th>Label</th>
                  <th>Created</th>
                </tr>
              </thead>
              <tbody>
                {patterns.map((p) => (
                  <tr key={p.id}>
                    <td>{p.id}</td>
                    <td>{p.symbol}</td>
                    <td>{p.timeframe}</td>
                    <td>{p.algo}</td>
                    <td>{p.cluster_size}</td>
                    <td>{p.avg_return != null ? Number(p.avg_return).toFixed(5) : ''}</td>
                    <td>{p.volatility != null ? Number(p.volatility).toFixed(5) : ''}</td>
                    <td>{p.label || ''}</td>
                    <td>{p.created_at ? new Date(p.created_at).toLocaleString() : ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>

        <section>
          <h3 style={{ margin: '8px 0' }}>Active Pattern & Regime</h3>
          {!active && !regime && <div style={{ color: '#9ca3af' }}>No active assignment or regime available yet.</div>}
          {(active || regime) && (
            <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
              {active && (
                <div style={{ minWidth: 260 }}>
                  <div style={{ color: '#9ca3af', fontSize: 12 }}>Active Assignment</div>
                  <div>Pattern ID: <b>{active.pattern_id}</b></div>
                  <div>Performance: <b>{active.performance != null ? Number(active.performance).toFixed(5) : '—'}</b></div>
                  <div>Window: <b>{active.start_ts ? new Date(active.start_ts).toLocaleTimeString() : '—'} → {active.end_ts ? new Date(active.end_ts).toLocaleTimeString() : '—'}</b></div>
                </div>
              )}
              {regime && (
                <div style={{ minWidth: 260 }}>
                  <div style={{ color: '#9ca3af', fontSize: 12 }}>Regime</div>
                  <div>State: <b>{regime.regime}</b></div>
                  <div>Confidence: <b>{regime.confidence != null ? Number(regime.confidence).toFixed(3) : '—'}</b></div>
                  <div>Model: <b>{regime.model_version || '—'}</b></div>
                </div>
              )}
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
