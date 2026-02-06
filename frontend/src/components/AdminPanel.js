import React, { useState } from 'react';
import axios from 'axios';
import './AdminPanel.css';

// Get API URL from environment variable or use default
const API_URL = process.env.REACT_APP_API_URL || '';

const AdminPanel = () => {
  const [apiKey, setApiKey] = useState('');
  const [apiSecret, setApiSecret] = useState('');
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [messageType, setMessageType] = useState(''); // 'success' or 'error'

  const handleRefresh = async () => {
    try {
      setLoading(true);
      setMessage('');
      
      // Call the API to update initial prices
      const response = await axios.post(`${API_URL}/api/coin-monitors/update-initial-prices`);
      
      setMessage(response.data.message || 'Successfully refreshed all coin data.');
      setMessageType('success');
    } catch (error) {
      console.error('Error refreshing data:', error);
      setMessage(error.response?.data?.detail || 'Error refreshing data. Please try again.');
      setMessageType('error');
    } finally {
      setLoading(false);
    }
  };

  const handleSaveCredentials = async () => {
    try {
      // Save credentials to localStorage
      localStorage.setItem('binanceApiKey', apiKey);
      localStorage.setItem('binanceApiSecret', apiSecret);

      await axios.post(`${API_URL}/api/trading/credentials`, {
        api_key: apiKey,
        api_secret: apiSecret
      });

      setMessage('Credentials saved successfully.');
      setMessageType('success');
    } catch (error) {
      console.error('Error saving credentials:', error);
      setMessage(error.response?.data?.detail || 'Error saving credentials. Please try again.');
      setMessageType('error');
    } finally {
      // Clear message after 3 seconds
      setTimeout(() => {
        setMessage('');
      }, 3000);
    }
  };

  // Load credentials from localStorage on component mount
  React.useEffect(() => {
    const savedApiKey = localStorage.getItem('binanceApiKey') || localStorage.getItem('binanceClientId');
    const savedApiSecret = localStorage.getItem('binanceApiSecret') || localStorage.getItem('binanceClientSecret');

    if (savedApiKey) setApiKey(savedApiKey);
    if (savedApiSecret) setApiSecret(savedApiSecret);
  }, []);

  return (
    <div className="admin-panel">
      <h2>Admin Panel</h2>
      
      <div className="credentials-section">
        <h3>Binance API Credentials</h3>
        <p>Enter your Binance API key/secret to enable live spot trading.</p>
        
        <div className="form-group">
          <label htmlFor="apiKey">API Key:</label>
          <input
            type="text"
            id="apiKey"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="Enter your Binance API key"
          />
        </div>
        
        <div className="form-group">
          <label htmlFor="apiSecret">API Secret:</label>
          <input
            type="password"
            id="apiSecret"
            value={apiSecret}
            onChange={(e) => setApiSecret(e.target.value)}
            placeholder="Enter your Binance API secret"
          />
        </div>
        
        <button 
          className="save-button"
          onClick={handleSaveCredentials}
          disabled={!apiKey || !apiSecret}
        >
          Save Credentials
        </button>
      </div>
      
      <div className="refresh-section">
        <h3>Refresh Coin Data</h3>
        <p>Click the button below to refresh all coin data. This will reset all coins to their current market prices.</p>
        
        <button 
          className="refresh-button"
          onClick={handleRefresh}
          disabled={loading}
        >
          {loading ? 'Refreshing...' : 'Refresh All Coin Data'}
        </button>
      </div>
      
      {message && (
        <div className={`message ${messageType}`}>
          {message}
        </div>
      )}
    </div>
  );
};

export default AdminPanel;
