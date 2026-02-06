import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import '../App.css';

const API_URL = process.env.REACT_APP_API_URL || '';

const zoneColor = (z) => z.zone_type === 'demand' ? 'rgba(34,197,94,0.18)' : 'rgba(248,113,113,0.18)';
const zoneBorder = (z) => z.zone_type === 'demand' ? '#22c55e' : '#ef4444';

export default function ZoneOverlayPanel({ coins = [], selectedSymbol }) {
  const [symbol, setSymbol] = useState(selectedSymbol || (coins[0] && coins[0].symbol) || '');
  const [timeframe, setTimeframe] = useState('1m');
  const [zones, setZones] = useState([]);
  const [plans, setPlans] = useState([]);
  const [portfolio, setPortfolio] = useState({ cash: 0, open_positions: [] });
  const [price, setPrice] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    if (selectedSymbol && selectedSymbol !== symbol) {
      setSymbol(selectedSymbol);
    } else if (!symbol && coins[0]) {
      setSymbol(coins[0].symbol);
    }
  }, [selectedSymbol, coins, symbol]);

  useEffect(() => {
    if (!symbol) return;
    const load = async () => {
      try {
        setLoading(true); setError('');
        const [zonesRes, plansRes, portfolioRes, priceRes] = await Promise.all([
          axios.get(`${API_URL}/api/zones`, { params: { symbol, timeframe, limit: 20 } }),
          axios.get(`${API_URL}/api/entry-plans`, { params: { symbol, limit: 20 } }),
          axios.get(`${API_URL}/api/trading/portfolio`),
          axios.get(`${API_URL}/api/coin-monitors/${symbol}`)
        ]);
        setZones(Array.isArray(zonesRes.data) ? zonesRes.data : []);
        setPlans(Array.isArray(plansRes.data) ? plansRes.data : []);
        setPortfolio(portfolioRes.data || { cash: 0, open_positions: [] });
        setPrice(priceRes.data?.latest_price || null);
      } catch (e) {
        setError(e.message || String(e));
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [symbol, timeframe]);

  const { scale, minVal, maxVal } = useMemo(() => {
    const values = [];
    zones.forEach(z => { values.push(z.proximal, z.distal); });
    if (price) values.push(price);
    if (values.length === 0) return { scale: () => 0.5, minVal: 0, maxVal: 1 };
    const minVal = Math.min(...values);
    const maxVal = Math.max(...values);
    const span = maxVal - minVal || 1;
    const scale = (v) => 1 - ((v - minVal) / span); // flip for chart top=high
    return { scale, minVal, maxVal };
  }, [zones, price]);

  const positions = Array.isArray(portfolio.open_positions) ? portfolio.open_positions : [];

  return (
    <div className="overlay-grid" style={{ width: '100%' }}>
      <div className="overlay-card">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
          <h3 style={{ margin: 0 }}>Zones Overlay</h3>
          {loading && <span style={{ fontSize: 12, color: '#9ca3af' }}>Loading…</span>}
        </div>
        <div className="overlay-controls">
          <select className="overlay-select" value={symbol} onChange={(e) => setSymbol(e.target.value)}>
            {coins.map(c => <option key={c.symbol} value={c.symbol}>{c.symbol}</option>)}
          </select>
          <select className="overlay-select" value={timeframe} onChange={(e) => setTimeframe(e.target.value)}>
            {['1m','5m','15m','30m','60m'].map(tf => <option key={tf} value={tf}>{tf}</option>)}
          </select>
          <div className="tag">Price: {price ? Number(price).toFixed(6) : '—'}</div>
        </div>
        {error && <div style={{ color: '#ef4444', marginBottom: 8 }}>Error: {error}</div>}
        <div className="zone-chart">
          {price !== null && (
            <div
              className="price-line"
              style={{ top: `${scale(price) * 100}%` }}
            />
          )}
          {zones.map((z) => {
            const top = scale(Math.max(z.proximal, z.distal)) * 100;
            const bottom = scale(Math.min(z.proximal, z.distal)) * 100;
            const height = Math.max(6, bottom - top);
            const qualityCls = z.quality_label === 'high' ? 'quality-high' : z.quality_label === 'medium' ? 'quality-medium' : 'quality-low';
            const probCls = z.probability_label === 'high' ? 'prob-high' : z.probability_label === 'medium' ? 'prob-medium' : 'prob-low';
            return (
              <div
                key={z.id}
                className="zone-bar"
                style={{
                  top: `${top}%`,
                  height: `${height}%`,
                  background: zoneColor(z),
                  borderColor: zoneBorder(z),
                }}
              >
                <div className="zone-label">
                  <span>{z.zone_type.toUpperCase()} · {z.formation}</span>
                  <div style={{ display: 'flex', gap: 6 }}>
                    <span className={`pill ${qualityCls}`}>{z.quality_label}</span>
                    <span className={`pill ${probCls}`}>{z.probability_label}</span>
                    <span className="pill">RR≈{z.rr_est ? z.rr_est.toFixed(1) : '—'}</span>
                  </div>
                </div>
                <div style={{ display: 'flex', gap: 6, marginTop: 4, fontSize: 11, color: '#cbd5e1' }}>
                  <span className="tag">curve: {z.curve_location || '—'}</span>
                  <span className="tag">trend: {z.trend || '—'}</span>
                </div>
              </div>
            );
          })}
          {plans.map(p => {
            const entryY = scale(p.entry_price) * 100;
            const stopY = scale(p.stop_price) * 100;
            const tpY = scale(p.take_profit_price) * 100;
            return (
              <React.Fragment key={p.id || `${p.symbol}-${p.entry_price}-${p.stop_price}`}>
                <div className="price-line" style={{ top: `${entryY}%`, background: 'linear-gradient(90deg, transparent, #22c55e, transparent)' }} />
                <div className="price-line" style={{ top: `${stopY}%`, background: 'linear-gradient(90deg, transparent, #ef4444, transparent)', opacity: 0.7 }} />
                <div className="price-line" style={{ top: `${tpY}%`, background: 'linear-gradient(90deg, transparent, #fbbf24, transparent)', opacity: 0.7 }} />
              </React.Fragment>
            );
          })}
        </div>
        <div className="legend">
          <span className="tag">Zones: shaded bars (green=demand, red=supply)</span>
          <span className="tag">Price line: cyan</span>
          <span className="tag">Plans: green(entry) · red(stop) · gold(TP)</span>
        </div>
        <div style={{ marginTop: 12, fontSize: 12, color: '#9ca3af' }}>
          Scale: {minVal.toFixed(6)} → {maxVal.toFixed(6)}
        </div>
      </div>

      <div className="overlay-card">
        <h3 style={{ marginTop: 0 }}>Plans & Positions</h3>
        <div className="plan-list">
          {plans.length === 0 && <div style={{ color: '#9ca3af' }}>No entry plans yet.</div>}
          {plans.map((p) => (
            <div key={p.id} style={{ padding: 10, border: '1px solid #1f2937', borderRadius: 8, background: '#0f172a' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <div style={{ fontWeight: 600 }}>{p.symbol} · {p.entry_type}</div>
                <div className="pill">{p.status}</div>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8, marginTop: 6, fontSize: 12, color: '#cbd5e1' }}>
                <div>Entry: <b>{Number(p.entry_price).toFixed(6)}</b></div>
                <div>Stop: <b>{Number(p.stop_price).toFixed(6)}</b></div>
                <div>TP: <b>{Number(p.take_profit_price).toFixed(6)}</b></div>
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3,1fr)', gap: 8, marginTop: 6, fontSize: 12, color: '#cbd5e1' }}>
                <div>Risk %: {p.risk_perc}</div>
                <div>RR: {p.rr_target}</div>
                <div>Size: {Number(p.position_size || 0).toFixed(4)}</div>
              </div>
            </div>
          ))}
        </div>
        <div style={{ marginTop: 12, borderTop: '1px solid #1f2937', paddingTop: 12 }}>
          <h4 style={{ margin: '0 0 6px' }}>Open Positions</h4>
          <div className="position-list">
            {positions.length === 0 && <div style={{ color: '#9ca3af' }}>No open positions.</div>}
            {positions.map((pos, idx) => (
              <div key={idx} style={{ padding: 10, border: '1px solid #1f2937', borderRadius: 8, background: '#0f172a' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                  <div style={{ fontWeight: 600 }}>{pos.symbol}</div>
                  <div className="pill">RR {pos.rr_target}</div>
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2,1fr)', gap: 8, marginTop: 6, fontSize: 12, color: '#cbd5e1' }}>
                  <div>Qty: {Number(pos.qty).toFixed(6)}</div>
                  <div>Entry: {Number(pos.entry_price).toFixed(6)}</div>
                  <div>Stop: {Number(pos.stop_price).toFixed(6)}</div>
                  <div>TP: {Number(pos.take_profit_price).toFixed(6)}</div>
                </div>
                <div style={{ display: 'flex', gap: 6, marginTop: 6, fontSize: 12 }}>
                  <span className="tag">breakeven: {pos.breakeven_set ? 'yes' : 'no'}</span>
                  <span className="tag">partial: {pos.partial_taken ? 'yes' : 'no'}</span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
