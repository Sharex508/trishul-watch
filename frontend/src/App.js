import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';
import CoinList from './components/CoinList';
import CoinDetail from './components/CoinDetail';
import AdminPanel from './components/AdminPanel';
import TradingPanel from './components/TradingPanel';
import PatternsPanel from './components/PatternsPanel';
import PredictionsPanel from './components/PredictionsPanel';
import ZoneOverlayPanel from './components/ZoneOverlayPanel';
import MonitorPanel from './components/MonitorPanel';

// Get API URL from environment variable or use default
const API_URL = process.env.REACT_APP_API_URL || '';

function App() {
  const [coins, setCoins] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedCoin, setSelectedCoin] = useState(null);
  const [showAdminPanel, setShowAdminPanel] = useState(false);
  const [tab, setTab] = useState('monitor'); // 'monitor' | 'trading' | 'patterns' | 'predictions' | 'zones' | 'monitoring'

  useEffect(() => {
    const fetchCoins = async () => {
      try {
        setLoading(true);
        const response = await axios.get(`${API_URL}/api/coin-monitors`);
        setCoins(response.data);

        // Check if there's a selected coin in localStorage
        const savedCoinSymbol = localStorage.getItem('selectedCoinSymbol');
        if (savedCoinSymbol) {
          // Find the coin with the saved symbol
          const savedCoin = response.data.find(coin => coin.symbol === savedCoinSymbol);
          if (savedCoin) {
            setSelectedCoin(savedCoin);
          }
        }

        setLoading(false);
      } catch (err) {
        setError('Error fetching coin data. Please try again later.');
        setLoading(false);
        console.error('Error fetching coin data:', err);
      }
    };

    fetchCoins();
    // Set up polling to refresh data every 20 seconds
    const interval = setInterval(fetchCoins, 20000);

    // Clean up interval on component unmount
    return () => clearInterval(interval);
  }, []);

  const handleCoinSelect = (coin) => {
    setSelectedCoin(coin);
    // Save selected coin to localStorage
    if (coin) {
      localStorage.setItem('selectedCoinSymbol', coin.symbol);
    }
  };

  const handleBack = () => {
    setSelectedCoin(null);
    // Clear selected coin from localStorage
    localStorage.removeItem('selectedCoinSymbol');
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>Coin Price Monitor</h1>
        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          <button className={`admin-button ${tab==='monitor'?'active':''}`} onClick={() => setTab('monitor')}>Monitor</button>
          <button className={`admin-button ${tab==='trading'?'active':''}`} onClick={() => setTab('trading')}>Trading</button>
          <button className={`admin-button ${tab==='patterns'?'active':''}`} onClick={() => setTab('patterns')}>Patterns</button>
          <button className={`admin-button ${tab==='predictions'?'active':''}`} onClick={() => setTab('predictions')}>Predictions</button>
          <button className={`admin-button ${tab==='zones'?'active':''}`} onClick={() => setTab('zones')}>Zones</button>
          <button className={`admin-button ${tab==='monitoring'?'active':''}`} onClick={() => setTab('monitoring')}>Monitor</button>
          <button 
            className="admin-button"
            onClick={() => setShowAdminPanel(!showAdminPanel)}
          >
            {showAdminPanel ? 'Hide Admin Panel' : 'Admin Panel'}
          </button>
        </div>
      </header>
      {showAdminPanel && <AdminPanel />}
      <main className="App-main">
        {tab === 'monitor' && (
          loading ? (
            <p>Loading coin data...</p>
          ) : error ? (
            <p className="error">{error}</p>
          ) : (
            <div className="content">
              <CoinList 
                coins={coins} 
                onSelectCoin={handleCoinSelect} 
                selectedCoin={selectedCoin}
              />
              {selectedCoin && <CoinDetail symbol={selectedCoin.symbol} onBack={handleBack} />}
            </div>
          )
        )}
        {tab === 'trading' && (
          <div className="content"><TradingPanel selectedSymbol={selectedCoin?.symbol} /></div>
        )}
        {tab === 'patterns' && (
          <div className="content"><PatternsPanel /></div>
        )}
        {tab === 'predictions' && (
          <div className="content"><PredictionsPanel /></div>
        )}
        {tab === 'zones' && (
          <div className="content"><ZoneOverlayPanel coins={coins} selectedSymbol={selectedCoin?.symbol} /></div>
        )}
        {tab === 'monitoring' && (
          <div className="content"><MonitorPanel /></div>
        )}
      </main>
      <footer className="App-footer">
        <p>Coin data refreshes automatically every 20 seconds.</p>
      </footer>
    </div>
  );
}

export default App;
