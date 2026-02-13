import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './CoinDetail.css';

// Get API URL from environment variable or use default
const API_URL = process.env.REACT_APP_API_URL || '';

const CoinDetail = ({ symbol, onBack }) => {
  const [coinHistory, setCoinHistory] = useState(null);
  const [recentTrades, setRecentTrades] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [buyAmount, setBuyAmount] = useState(0);
  const [sellAmount, setSellAmount] = useState(0);
  const [buyPercentage, setBuyPercentage] = useState(0);
  const [sellPercentage, setSellPercentage] = useState(0);
  const [tradeLoading, setTradeLoading] = useState(false);
  const [tradeMessage, setTradeMessage] = useState('');
  const [tradeMessageType, setTradeMessageType] = useState(''); // 'success' or 'error'

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);

        // Fetch both history and recent trades in parallel
        const [historyResponse, tradesResponse] = await Promise.all([
          axios.get(`${API_URL}/api/coin-monitors/${symbol}/history`),
          axios.get(`${API_URL}/api/coin-monitors/${symbol}/recent-trades`)
        ]);

        setCoinHistory(historyResponse.data);
        setRecentTrades(tradesResponse.data);
        setLoading(false);
      } catch (err) {
        setError(`Error fetching data for ${symbol}`);
        setLoading(false);
        console.error(`Error fetching data for ${symbol}:`, err);
      }
    };

    if (symbol) {
      fetchData();
    }

    // Set up polling to refresh data every 3 seconds
    const interval = setInterval(() => {
      if (symbol) {
        fetchData();
      }
    }, 3000);

    // Clean up interval on component unmount or when symbol changes
    return () => clearInterval(interval);
  }, [symbol]);

  if (!symbol) {
    return <div className="coin-detail">Select a coin to view details</div>;
  }

  if (loading) {
    return <div className="coin-detail">Loading {symbol} details...</div>;
  }

  if (error) {
    return <div className="coin-detail error">{error}</div>;
  }

  if (!coinHistory) {
    return <div className="coin-detail">No history available for {symbol}</div>;
  }

  const handleBuyPercentageChange = (e) => {
    const percentage = parseInt(e.target.value);
    setBuyPercentage(percentage);
    // Calculate amount based on percentage (assuming a max amount of 1000 for example)
    setBuyAmount((percentage / 100) * 1000);
  };

  const handleSellPercentageChange = (e) => {
    const percentage = parseInt(e.target.value);
    setSellPercentage(percentage);
    // Calculate amount based on percentage (assuming a max amount of 1000 for example)
    setSellAmount((percentage / 100) * 1000);
  };

  const handleBuy = async () => {
    // Get credentials from localStorage
    const apiKey = localStorage.getItem('binanceApiKey') || localStorage.getItem('binanceClientId');
    const apiSecret = localStorage.getItem('binanceApiSecret') || localStorage.getItem('binanceClientSecret');

    if (!apiKey || !apiSecret) {
      setTradeMessage('Please set your Binance API key/secret in the Admin Panel first.');
      setTradeMessageType('error');
      return;
    }

    if (buyAmount <= 0) {
      setTradeMessage('Please select an amount to buy.');
      setTradeMessageType('error');
      return;
    }

    try {
      setTradeLoading(true);
      setTradeMessage('');

      const response = await axios.post(`${API_URL}/api/trade/buy`, {
        symbol,
        amount: buyAmount,
        api_key: apiKey,
        api_secret: apiSecret
      });

      setTradeMessage(response.data.message);
      setTradeMessageType('success');
    } catch (error) {
      console.error('Error buying coin:', error);
      setTradeMessage(error.response?.data?.detail || 'Error buying coin. Please try again.');
      setTradeMessageType('error');
    } finally {
      setTradeLoading(false);
    }
  };

  const handleSell = async () => {
    // Get credentials from localStorage
    const apiKey = localStorage.getItem('binanceApiKey') || localStorage.getItem('binanceClientId');
    const apiSecret = localStorage.getItem('binanceApiSecret') || localStorage.getItem('binanceClientSecret');

    if (!apiKey || !apiSecret) {
      setTradeMessage('Please set your Binance API key/secret in the Admin Panel first.');
      setTradeMessageType('error');
      return;
    }

    if (sellAmount <= 0) {
      setTradeMessage('Please select an amount to sell.');
      setTradeMessageType('error');
      return;
    }

    try {
      setTradeLoading(true);
      setTradeMessage('');

      const response = await axios.post(`${API_URL}/api/trade/sell`, {
        symbol,
        amount: sellAmount,
        api_key: apiKey,
        api_secret: apiSecret
      });

      setTradeMessage(response.data.message);
      setTradeMessageType('success');
    } catch (error) {
      console.error('Error selling coin:', error);
      setTradeMessage(error.response?.data?.detail || 'Error selling coin. Please try again.');
      setTradeMessageType('error');
    } finally {
      setTradeLoading(false);
    }
  };

  return (
    <div className="coin-detail">
      <div className="detail-header">
        <button className="back-button" onClick={onBack}>← Back</button>
        <h2>{symbol} Details</h2>
      </div>

      {coinHistory.moving_averages && (
        <div className="trend-analysis">
          <h3>Trend Analysis</h3>
          <div className="moving-averages">
            <div className="ma-card">
              <h4>MA(7)</h4>
              <p className="ma-value">${coinHistory.moving_averages.ma7.toFixed(7)}</p>
            </div>
            <div className="ma-card">
              <h4>MA(25)</h4>
              <p className="ma-value">${coinHistory.moving_averages.ma25.toFixed(7)}</p>
            </div>
            <div className="ma-card">
              <h4>MA(99)</h4>
              <p className="ma-value">${coinHistory.moving_averages.ma99.toFixed(7)}</p>
            </div>
          </div>

          <div className="trend-info">
            <div className={`trend-card ${coinHistory.trend_analysis.trend.toLowerCase()}`}>
              <h4>Trend</h4>
              <p className="trend-value">{coinHistory.trend_analysis.trend}</p>
            </div>
            <div className="cycle-card">
              <h4>Cycle Status</h4>
              <p className="cycle-value">{coinHistory.trend_analysis.cycle_status}</p>
            </div>
          </div>
        </div>
      )}

      {recentTrades && (
        <div className="recent-trades">
          <h3>Last 30 Seconds Trading Activity</h3>
          <div className="trade-stats">
            <div className="trade-card">
              <h4>Total Trades</h4>
              <p className="trade-value">{recentTrades.total_trades}</p>
            </div>
            <div className="trade-card">
              <h4>Buy Trades</h4>
              <p className="trade-value buy">{recentTrades.buy_trades} ({recentTrades.buy_percentage}%)</p>
            </div>
            <div className="trade-card">
              <h4>Sell Trades</h4>
              <p className="trade-value sell">{recentTrades.sell_trades} ({recentTrades.sell_percentage}%)</p>
            </div>
            <div className="trade-card">
              <h4>Buy Volume</h4>
              <p className="trade-value buy">{recentTrades.buy_volume}</p>
            </div>
            <div className="trade-card">
              <h4>Sell Volume</h4>
              <p className="trade-value sell">{recentTrades.sell_volume}</p>
            </div>
            <div className="trade-card">
              <h4>Avg Trade Size</h4>
              <p className="trade-value">{recentTrades.average_trade_size}</p>
            </div>
          </div>

          <div className="trade-analysis">
            <div className={`trend-indicator ${recentTrades.trend.toLowerCase()}`}>
              <h4>Market Sentiment</h4>
              <p className="trend-value">{recentTrades.trend}</p>
            </div>
            <div className="binance-link">
              <h4>Trade on Binance</h4>
              <a href={recentTrades.binance_link} target="_blank" rel="noopener noreferrer" className="binance-button">
                Open {symbol} on Binance
              </a>
            </div>
          </div>
        </div>
      )}

      <div className="trade-section">
        <h3>Buy/Sell {symbol}</h3>
        <div className="trade-actions">
          <div className="buy-section">
            <h4>Buy {symbol}</h4>
            <div className="percentage-slider">
              <label htmlFor="buyPercentage">Amount: {buyPercentage}% (${buyAmount.toFixed(2)})</label>
              <input
                type="range"
                id="buyPercentage"
                min="0"
                max="100"
                step="5"
                value={buyPercentage}
                onChange={handleBuyPercentageChange}
                className="slider"
              />
            </div>
            <button 
              className="buy-button"
              onClick={handleBuy}
              disabled={tradeLoading || buyAmount <= 0}
            >
              {tradeLoading ? 'Processing...' : `Buy ${symbol}`}
            </button>
          </div>
          <div className="sell-section">
            <h4>Sell {symbol}</h4>
            <div className="percentage-slider">
              <label htmlFor="sellPercentage">Amount: {sellPercentage}% (${sellAmount.toFixed(2)})</label>
              <input
                type="range"
                id="sellPercentage"
                min="0"
                max="100"
                step="5"
                value={sellPercentage}
                onChange={handleSellPercentageChange}
                className="slider"
              />
            </div>
            <button 
              className="sell-button"
              onClick={handleSell}
              disabled={tradeLoading || sellAmount <= 0}
            >
              {tradeLoading ? 'Processing...' : `Sell ${symbol}`}
            </button>
          </div>
        </div>

        {tradeMessage && (
          <div className={`trade-message ${tradeMessageType}`}>
            {tradeMessage}
          </div>
        )}
      </div>

      <h3>Price History</h3>
      {coinHistory.history.length === 0 ? (
        <p>No price history available yet</p>
      ) : (
        <div className="history-table">
          <div className="history-header">
            <span>Set</span>
            <span>Previous Cycle High</span>
            <span>Low</span>
            <span>High</span>
            <span>Range</span>
          </div>
          <div className="history-body">
            {coinHistory.history.map((item) => {
              const range = ((item.high_price - item.low_price) / item.low_price * 100).toFixed(2);
              return (
                <div key={item.set} className="history-row">
                  <span>{item.set}</span>
                  <span>{item.prev_cycle_high ? `$${item.prev_cycle_high.toFixed(7)}` : '-'}</span>
                  <span>${item.low_price.toFixed(7)}</span>
                  <span>${item.high_price.toFixed(7)}</span>
                  <span>{range}%</span>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="price-info">
        <div className="price-card">
          <h3>Current Price</h3>
          <p className="price-value">${coinHistory.current.latest_price.toFixed(7)}</p>
        </div>

        <div className="price-card">
          <h3>Initial Price</h3>
          <p className="price-value">${coinHistory.initial_price.toFixed(7)}</p>
        </div>

        <div className="price-card">
          <h3>24h High</h3>
          <p className="price-value">${coinHistory.current.high_price.toFixed(7)}</p>
        </div>

        <div className="price-card">
          <h3>24h Low</h3>
          <p className="price-value">${coinHistory.current.low_price.toFixed(7)}</p>
        </div>
      </div>

      <div className="timestamp">
        Last updated: {new Date(coinHistory.updated_at).toLocaleString()}
      </div>
    </div>
  );
};

export default CoinDetail;
