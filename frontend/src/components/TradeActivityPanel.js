import React, { useEffect, useState } from 'react';

const API_URL = process.env.REACT_APP_API_URL || '';

export default function TradeActivityPanel() {
  const [rows, setRows] = useState([]);
  const [error, setError] = useState('');
  const [limit, setLimit] = useState(50);
  const [filter, setFilter] = useState('');

  const load = async () => {
    try {
      const res = await fetch(`${API_URL}/api/trade-activity?limit=${limit}`);
      const json = await res.json();
      setRows(Array.isArray(json) ? json : []);
      setError('');
    } catch (e) {
      setError(e.message || String(e));
    }
  };

  useEffect(() => {
    load();
    const id = setInterval(load, 5000);
    return () => clearInterval(id);
  }, [limit]);

  const filtered = rows.filter(r => !filter || (r.symbol || '').toLowerCase().includes(filter.toLowerCase()));

  return (
    <div className="content" style={{ display: 'grid', gap: 16 }}>
      <div className="overlay-card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <h2 style={{ margin: 0 }}>Trade Activity</h2>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <input
              className="overlay-input"
              placeholder="Filter symbol..."
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              style={{ width: 180 }}
            />
            <input
              type="number"
              className="overlay-input"
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value) || 50)}
              style={{ width: 100 }}
              min={1}
              max={200}
            />
          </div>
        </div>
        {error && <div style={{ color: '#ef4444', marginTop: 8 }}>Error: {error}</div>}
        <div style={{ marginTop: 12, maxHeight: 500, overflowY: 'auto' }}>
          <table className="data-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th>Total Trades (1m)</th>
                <th>Buy</th>
                <th>Sell</th>
                <th>Buy Vol</th>
                <th>Sell Vol</th>
                <th>Last Update</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r) => {
                const ageSec = r.ts ? Math.max(0, Math.floor((Date.now() - r.ts) / 1000)) : '-';
                return (
                  <tr key={r.symbol}>
                    <td>{r.symbol}</td>
                    <td>{r.total_trades}</td>
                    <td>{r.buy_count}</td>
                    <td>{r.sell_count}</td>
                    <td>{Number(r.buy_volume || 0).toFixed(4)}</td>
                    <td>{Number(r.sell_volume || 0).toFixed(4)}</td>
                    <td>{ageSec}s ago</td>
                  </tr>
                );
              })}
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={7} style={{ color: '#9ca3af', textAlign: 'center' }}>No data yet.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
