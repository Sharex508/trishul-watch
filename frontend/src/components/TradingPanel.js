import React, { useEffect, useState } from 'react';
import './AdminPanel.css';

const API_URL = process.env.REACT_APP_API_URL || '';

export default function TradingPanel({ selectedSymbol }) {
  const [status, setStatus] = useState({ enabled: false });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [logs, setLogs] = useState([]);
  const [logError, setLogError] = useState('');
  const [portfolio, setPortfolio] = useState(null);
  const [coins, setCoins] = useState([]);
  const [strategy, setStrategy] = useState(() => {
    try {
      const raw = localStorage.getItem('trading_prefs');
      if (raw) {
        const prefs = JSON.parse(raw);
        return prefs.strategy || 'coin';
      }
    } catch (_) {}
    return 'coin';
  }); // coin | intraday | ai
  const [paper, setPaper] = useState(() => {
    try {
      const raw = localStorage.getItem('trading_prefs');
      if (raw) {
        const prefs = JSON.parse(raw);
        if (typeof prefs.paper === 'boolean') return prefs.paper;
      }
    } catch (_) {}
    return true;
  });
  const [symbol, setSymbol] = useState(() => {
    try {
      const raw = localStorage.getItem('trading_prefs');
      if (raw) {
        const prefs = JSON.parse(raw);
        if (prefs.symbol) return prefs.symbol;
      }
    } catch (_) {}
    return '';
  });
  const [symbolQuery, setSymbolQuery] = useState('');
  const [intradayAmount, setIntradayAmount] = useState('');
  const [intradayTrades, setIntradayTrades] = useState('');
  const [intradayProfit, setIntradayProfit] = useState('');
  const [intradayAvoidTop, setIntradayAvoidTop] = useState('');
  const [intradayPumpPullback, setIntradayPumpPullback] = useState(false);
  const [intradayTradesFilter, setIntradayTradesFilter] = useState(true);
  const [intradayMinTrades, setIntradayMinTrades] = useState('50');
  const [intradayMsg, setIntradayMsg] = useState('');

  // Load persisted preferences
  useEffect(() => {
    try {
      const raw = localStorage.getItem('trading_prefs');
      if (raw) {
        const prefs = JSON.parse(raw);
        if (prefs.strategy) setStrategy(prefs.strategy);
        if (typeof prefs.paper === 'boolean') setPaper(prefs.paper);
        if (prefs.symbol) setSymbol(prefs.symbol);
      }
    } catch (e) {
      // ignore parse errors
    }
  }, []);

  const loadStatus = async () => {
    try {
      const res = await fetch(`${API_URL}/api/trading/status`);
      const json = await res.json();
      setStatus(json);
    } catch (e) {
      // ignore transient
    }
  };

  const loadIntradayLimits = async () => {
    try {
      const res = await fetch(`${API_URL}/api/trading/intraday-limits`);
      const json = await res.json();
      if (json && typeof json === 'object') {
        if (json.amount !== undefined) setIntradayAmount(json.amount);
        if (json.number_of_trades !== undefined) setIntradayTrades(json.number_of_trades);
        if (json.profit !== undefined) setIntradayProfit(json.profit);
        if (json.avoid_top_pct !== undefined) setIntradayAvoidTop(json.avoid_top_pct);
        if (json.pump_pullback_enabled !== undefined) setIntradayPumpPullback(Boolean(json.pump_pullback_enabled));
        if (json.trades_filter_enabled !== undefined) setIntradayTradesFilter(Boolean(json.trades_filter_enabled));
        if (json.min_trades_1m !== undefined) setIntradayMinTrades(json.min_trades_1m);
      }
    } catch (e) {
      // ignore
    }
  };

  const loadCoins = async () => {
    try {
      const res = await fetch(`${API_URL}/api/coin-monitors`);
      const json = await res.json();
      if (Array.isArray(json)) {
        setCoins(json);
        if (!symbol && json.length > 0) {
          setSymbol(json[0].symbol);
        }
      }
    } catch (e) {
      // ignore
    }
  };

  const loadLogs = async () => {
    try {
      const res = await fetch(`${API_URL}/api/trade-logs?limit=200`);
      const json = await res.json();
      setLogs(Array.isArray(json) ? json : []);
      setLogError('');
    } catch (e) {
      setLogError(e.message || String(e));
    }
  };

  const loadPortfolio = async () => {
    try {
      const res = await fetch(`${API_URL}/api/trading/portfolio`);
      const json = await res.json();
      setPortfolio(json);
    } catch (e) {
      // ignore
    }
  };

  useEffect(() => {
    loadStatus();
    loadLogs();
    loadPortfolio();
    loadCoins();
    loadIntradayLimits();
    const id = setInterval(loadStatus, 5000);
    const id2 = setInterval(loadLogs, 5000);
    const id3 = setInterval(loadPortfolio, 5000);
    return () => { clearInterval(id); clearInterval(id2); clearInterval(id3); };
  }, []);

  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
      setSymbolQuery('');
    }
  }, [selectedSymbol, symbol]);

  // Auto-select when search narrows to a single match
  useEffect(() => {
    const matches = coins.filter(c => c.symbol.toLowerCase().includes(symbolQuery.toLowerCase()));
    if (matches.length === 1) {
      setSymbol(matches[0].symbol);
    }
  }, [symbolQuery, coins]);

  // Persist preferences
  useEffect(() => {
    try {
      localStorage.setItem('trading_prefs', JSON.stringify({ strategy, paper, symbol }));
    } catch (e) {
      // ignore
    }
  }, [strategy, paper, symbol]);

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

  const totalPnl = logs.reduce((acc, t) => acc + Number(t.pnl || 0), 0);

  const startSelected = async () => {
    setBusy(true); setError(''); setMessage('');
    try {
      if (strategy === 'coin') {
        const res = await fetch(`${API_URL}/api/trading/coin-start`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ symbol, paper })
        });
        if (!res.ok) throw new Error(await res.text());
        const json = await res.json();
        setStatus((prev) => ({ ...prev, ...json }));
        setMessage(`Coin Brain started for ${symbol} (${paper ? 'paper' : 'live'})`);
      } else if (strategy === 'intraday') {
        const res = await fetch(`${API_URL}/api/trading/intraday-start`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ paper })
        });
        if (!res.ok) throw new Error(await res.text());
        const json = await res.json();
        setStatus((prev) => ({ ...prev, ...json }));
        setMessage('Intraday trading started.');
      } else {
        const res = await fetch(`${API_URL}/api/trading/start`, { method: 'POST' });
        if (!res.ok) throw new Error(await res.text());
        const json = await res.json();
        setStatus((prev) => ({ ...prev, ...json }));
        setMessage('AI trading started (paper loop).');
      }
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  const saveIntradayLimits = async () => {
    setBusy(true); setError(''); setIntradayMsg('');
    try {
      const payload = {
        amount: intradayAmount === '' ? undefined : Number(intradayAmount),
        number_of_trades: intradayTrades === '' ? undefined : Number(intradayTrades),
        profit: intradayProfit === '' ? undefined : Number(intradayProfit),
        avoid_top_pct: intradayAvoidTop === '' ? undefined : Number(intradayAvoidTop),
        pump_pullback_enabled: intradayPumpPullback ? 1 : 0,
        trades_filter_enabled: intradayTradesFilter ? 1 : 0,
        min_trades_1m: intradayMinTrades === '' ? undefined : Number(intradayMinTrades),
      };
      const res = await fetch(`${API_URL}/api/trading/intraday-limits`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      if (!res.ok) throw new Error(await res.text());
      const json = await res.json();
      if (json.amount !== undefined) setIntradayAmount(json.amount);
      if (json.number_of_trades !== undefined) setIntradayTrades(json.number_of_trades);
      if (json.profit !== undefined) setIntradayProfit(json.profit);
      if (json.avoid_top_pct !== undefined) setIntradayAvoidTop(json.avoid_top_pct);
      if (json.pump_pullback_enabled !== undefined) setIntradayPumpPullback(Boolean(json.pump_pullback_enabled));
      if (json.trades_filter_enabled !== undefined) setIntradayTradesFilter(Boolean(json.trades_filter_enabled));
      if (json.min_trades_1m !== undefined) setIntradayMinTrades(json.min_trades_1m);
      setIntradayMsg('Intraday limits updated.');
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  const toggleHybrid = async (enabled) => {
    setBusy(true); setError(''); setMessage('');
    try {
      const res = await fetch(`${API_URL}/api/trading/hybrid`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled })
      });
      if (!res.ok) throw new Error(await res.text());
      const json = await res.json();
      setStatus((prev) => ({ ...prev, ...json }));
      setMessage(`Hybrid mode ${enabled ? 'enabled' : 'disabled'}.`);
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    if (strategy === 'intraday' && status.hybrid_enabled && !busy) {
      toggleHybrid(false);
    }
  }, [strategy, status.hybrid_enabled]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="content" style={{ display: 'grid', gap: 16, gridTemplateColumns: '1fr 2fr' }}>
      <div className="admin-panel">
        <h2>Trading Controls</h2>
        <p>
          Status: {status.enabled ? (
            <span style={{ color: '#22c55e' }}>ENABLED</span>
          ) : (
            <span style={{ color: '#f59e0b' }}>DISABLED</span>
          )}
          {status.strategy_mode && (
            <span style={{ marginLeft: 8, fontSize: 12, color: '#9ca3af' }}>mode: {status.strategy_mode}</span>
          )}
          {'hybrid_enabled' in status && (
            <span style={{ marginLeft: 8, fontSize: 12, color: '#9ca3af' }}>
              hybrid: {status.hybrid_enabled ? 'on' : 'off'}
            </span>
          )}
        </p>
        <div style={{ display: 'grid', gap: 10, margin: '10px 0', textAlign: 'left' }}>
          <div>
            <label style={{ display: 'block', fontSize: 13, color: '#9ca3af', marginBottom: 4 }}>Strategy</label>
            <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
              {['coin','intraday','ai'].map(s => (
                <label key={s} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  <input type="radio" name="strategy" value={s} checked={strategy === s} onChange={() => setStrategy(s)} /> {s.toUpperCase()}
                </label>
              ))}
            </div>
          </div>
          {strategy === 'coin' && (
            <div>
              <label style={{ display: 'block', fontSize: 13, color: '#9ca3af', marginBottom: 4 }}>Symbol</label>
              <input
                type="text"
                className="overlay-input"
                placeholder="Search symbol..."
                value={symbolQuery}
                onChange={(e) => setSymbolQuery(e.target.value)}
                style={{ width: '100%', marginBottom: 8 }}
              />
              <select value={symbol} onChange={(e) => setSymbol(e.target.value)} className="overlay-select" style={{ width: '100%' }}>
                {coins
                  .filter(c => c.symbol.toLowerCase().includes(symbolQuery.toLowerCase()))
                  .map(c => <option key={c.symbol} value={c.symbol}>{c.symbol}</option>)}
              </select>
            </div>
          )}
          {strategy === 'intraday' && (
            <div style={{ display: 'grid', gap: 8 }}>
              <label style={{ display: 'block', fontSize: 13, color: '#9ca3af' }}>Amount (USDT)</label>
              <input
                type="number"
                className="overlay-input"
                value={intradayAmount}
                onChange={(e) => setIntradayAmount(e.target.value)}
                placeholder="e.g. 100"
              />
              <label style={{ display: 'block', fontSize: 13, color: '#9ca3af' }}>Number of trades</label>
              <input
                type="number"
                className="overlay-input"
                value={intradayTrades}
                onChange={(e) => setIntradayTrades(e.target.value)}
                placeholder="Max open trades (0 = unlimited)"
              />
              <label style={{ display: 'block', fontSize: 13, color: '#9ca3af' }}>Take profit % (net)</label>
              <input
                type="number"
                className="overlay-input"
                value={intradayProfit}
                onChange={(e) => setIntradayProfit(e.target.value)}
                placeholder="e.g. 0.5"
              />
              <label style={{ display: 'block', fontSize: 13, color: '#9ca3af' }}>Avoid top %</label>
              <input
                type="number"
                className="overlay-input"
                value={intradayAvoidTop}
                onChange={(e) => setIntradayAvoidTop(e.target.value)}
                placeholder="e.g. 1.0"
              />
              <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13 }}>
                <input
                  type="checkbox"
                  checked={intradayPumpPullback}
                  onChange={(e) => setIntradayPumpPullback(e.target.checked)}
                />
                Pump→pullback filter (require pullback + bounce after 30m pump)
              </label>
              <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13 }}>
                <input
                  type="checkbox"
                  checked={intradayTradesFilter}
                  onChange={(e) => setIntradayTradesFilter(e.target.checked)}
                />
                Min trades in last 1m (default 50)
              </label>
              {intradayTradesFilter && (
                <input
                  type="number"
                  className="overlay-input"
                  value={intradayMinTrades}
                  onChange={(e) => setIntradayMinTrades(e.target.value)}
                  placeholder="e.g. 50"
                />
              )}
              <button disabled={busy} onClick={saveIntradayLimits} className="admin-button">Update intraday limits</button>
              {intradayMsg && <div style={{ color: '#22c55e', fontSize: 12 }}>{intradayMsg}</div>}
            </div>
          )}
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13 }}>
            <input type="checkbox" checked={paper} onChange={(e) => setPaper(e.target.checked)} /> Paper trading (simulate, log P/L)
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13 }}>
            <input
              type="checkbox"
              checked={strategy === 'intraday' ? false : !!status.hybrid_enabled}
              onChange={(e) => toggleHybrid(e.target.checked)}
              disabled={busy || strategy === 'intraday'}
            />{' '}
            Hybrid mode (AI decide; unchecked disables hybrid loop)
          </label>
        </div>
        <div style={{ display: 'flex', gap: '10px', margin: '10px 0', flexWrap: 'wrap' }}>
          <button disabled={busy} onClick={startSelected} className="admin-button">Start</button>
          <button disabled={busy} onClick={() => call('stop')} className="admin-button">Stop</button>
          <button disabled={busy} onClick={() => call('reset')} className="admin-button">Reset</button>
        </div>
        <p style={{ fontSize: 12, color: '#9ca3af' }}>
          Select an algorithm, optionally pick a symbol for Coin mode, toggle paper trading, then Start. Stop halts it; Reset clears all completed trade entries.
        </p>
        {message && <div style={{ color: '#22c55e', marginTop: 8 }}>{message}</div>}
        {error && <div style={{ color: '#ef4444', marginTop: 8 }}>Error: {error}</div>}
        <div style={{ marginTop: 16, padding: 10, background: '#0f172a', borderRadius: 8, border: '1px solid #1f2937' }}>
          <div style={{ color: '#9ca3af', fontSize: 12 }}>Total P&L (paper)</div>
          <div style={{ fontWeight: 700, color: totalPnl >= 0 ? '#22c55e' : '#ef4444', fontSize: 18 }}>
            {totalPnl >= 0 ? '+' : ''}{totalPnl.toFixed(4)}
          </div>
          {portfolio && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#9ca3af' }}>
              Cash: <b style={{ color: '#e5e7eb' }}>{Number(portfolio.cash || 0).toFixed(2)}</b> ·
              Open positions: <b style={{ color: '#e5e7eb' }}>{(portfolio.open_positions || []).length}</b> ·
              Unrealized P&L: <b style={{ color: Number(portfolio.unrealized_pnl || 0) >= 0 ? '#22c55e' : '#ef4444' }}>{Number(portfolio.unrealized_pnl || 0).toFixed(4)}</b> ·
              Unrealized %: <b style={{ color: Number(portfolio.unrealized_pct || 0) >= 0 ? '#22c55e' : '#ef4444' }}>{Number(portfolio.unrealized_pct || 0).toFixed(2)}%</b> ·
              Equity (cash + unrealized): <b style={{ color: '#e5e7eb' }}>{(Number(portfolio.cash || 0) + Number(portfolio.unrealized_pnl || 0)).toFixed(2)}</b>
            </div>
          )}
        </div>
      </div>

      <div className="admin-panel">
        <h2>Trade Logs</h2>
        <p style={{ fontSize: 12, color: '#9ca3af' }}>Completed paper trades. P&L above is SELL cash in minus BUY cash out (demo).</p>
        {logError && <div style={{ color: '#ef4444' }}>Error: {logError}</div>}
        <div style={{ marginBottom: 12, overflowX: 'auto', maxHeight: 220 }}>
          <table className="admin-table">
            <thead>
              <tr>
                <th>Open Orders</th>
                <th>Entry</th>
                <th>Current</th>
                <th>Qty</th>
                <th>Notional</th>
                <th>Unrealized (USDT)</th>
                <th>Unrealized %</th>
                <th>Stop</th>
                <th>TP</th>
              </tr>
            </thead>
            <tbody>
              {(!portfolio || (portfolio.open_positions || []).length === 0) && (
                <tr><td colSpan={9} style={{ color: '#9ca3af' }}>No open orders.</td></tr>
              )}
              {(portfolio?.open_positions || []).map((p) => (
                <tr key={`${p.symbol}-${p.entry_price}`}>
                  <td>{p.symbol}</td>
                  <td>{Number(p.entry_price).toFixed(8)}</td>
                  <td>{Number(p.current_price || p.entry_price).toFixed(8)}</td>
                  <td>{Number(p.qty).toFixed(6)}</td>
                  <td>{Number(p.notional || 0).toFixed(2)}</td>
                  <td style={{ color: Number(p.unrealized_pnl || 0) >= 0 ? '#22c55e' : '#ef4444' }}>
                    {Number(p.unrealized_pnl || 0).toFixed(4)}
                  </td>
                  <td style={{ color: Number(p.unrealized_pct || 0) >= 0 ? '#22c55e' : '#ef4444' }}>
                    {Number(p.unrealized_pct || 0).toFixed(2)}%
                  </td>
                  <td>{p.stop_price ? Number(p.stop_price).toFixed(8) : '-'}</td>
                  <td>{p.take_profit_price ? Number(p.take_profit_price).toFixed(8) : '-'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div style={{ overflowX: 'auto', maxHeight: 400 }}>
          <table className="admin-table">
            <thead>
              <tr>
                <th>ID</th>
                <th>Time</th>
                <th>Symbol</th>
                <th>Side</th>
                <th>Qty</th>
                <th>Price</th>
                <th>Notional (USDT)</th>
                <th>P&L (USDT)</th>
                <th>P&L %</th>
                <th>Reason</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {logs.length === 0 && (
                <tr><td colSpan={8} style={{ color: '#9ca3af' }}>No trades yet. Click Start to begin.</td></tr>
              )}
              {logs.map((t) => {
                const qty = Number(t.qty || 0);
                const pnl = Number(t.pnl || 0);
                const exitPrice = Number(t.price || 0);
                let entryPrice = 0;
                let pnlPct = 0;
                if (t.side === 'SELL' && qty > 0) {
                  entryPrice = exitPrice - pnl / qty;
                  if (entryPrice > 0) {
                    pnlPct = (pnl / (entryPrice * qty)) * 100;
                  }
                }
                return (
                <tr key={t.id}>
                  <td>{t.id}</td>
                  <td>{t.created_at ? new Date(t.created_at).toLocaleString() : ''}</td>
                  <td>{t.symbol}</td>
                  <td style={{ color: t.side === 'BUY' ? '#22c55e' : '#ef4444' }}>{t.side}</td>
                  <td>{Number(t.qty).toFixed(6)}</td>
                  <td>{Number(t.price).toFixed(8)}</td>
                  <td>{(Number(t.qty) * Number(t.price)).toFixed(2)}</td>
                  <td style={{ color: Number(t.pnl || 0) >= 0 ? '#22c55e' : '#ef4444' }}>
                    {Number(t.pnl || 0).toFixed(4)}
                  </td>
                  <td style={{ color: pnlPct >= 0 ? '#22c55e' : '#ef4444' }}>
                    {pnlPct ? `${pnlPct.toFixed(2)}%` : '-'}
                  </td>
                  <td>{t.reason}</td>
                  <td>{t.status}</td>
                </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
