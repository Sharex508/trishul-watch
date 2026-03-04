import React, { useState } from 'react';
import './CoinList.css';

const CoinList = ({ coins, onSelectCoin, selectedCoin }) => {
  const [activeTab, setActiveTab] = useState('all');
  const [sortField, setSortField] = useState('change');
  const [sortDirection, setSortDirection] = useState('desc'); // 'asc' or 'desc'
  const [searchTerm, setSearchTerm] = useState(''); // For search functionality

  // Separate coins into rising and falling
  // A coin is considered falling if it has dropped 0.5% or more from its peak (high_price)
  const fallingThreshold = 0.995; // 0.5% drop
  const risingCoins = coins.filter(coin => coin.latest_price >= coin.high_price * fallingThreshold);
  const fallingCoins = coins.filter(coin => coin.latest_price < coin.high_price * fallingThreshold);

  // Filter coins based on search term
  const filterBySearchTerm = (coinsArray) => {
    if (!searchTerm.trim()) return coinsArray;
    return coinsArray.filter(coin => 
      coin.symbol.toLowerCase().includes(searchTerm.toLowerCase())
    );
  };

  const filteredCoins = filterBySearchTerm(coins);
  const filteredRisingCoins = filterBySearchTerm(risingCoins);
  const filteredFallingCoins = filterBySearchTerm(fallingCoins);

  // Helper function to safely calculate percentage change
  const calculatePercentChange = (latest, initial) => {
    if (!initial || initial === 0) return 0;
    return ((latest - initial) / initial) * 100;
  };

  // Sort function for coins
  const sortCoins = (coinsToSort, customDirection, isFalling = false) => {
    // Use the provided customDirection if available, otherwise use the state sortDirection
    const direction = customDirection || sortDirection;

    return [...coinsToSort].sort((a, b) => {
      let aValue, bValue;

      if (sortField === 'change') {
        aValue = calculatePercentChange(a.latest_price, a.initial_price);
        bValue = calculatePercentChange(b.latest_price, b.initial_price);

        // For falling coins, we want to sort by the absolute value of the percentage change
        // This ensures coins with the largest percentage decrease (most negative values) appear at the top
        if (isFalling) {
          aValue = Math.abs(aValue);
          bValue = Math.abs(bValue);
        }
      } else if (sortField === 'price') {
        aValue = a.latest_price;
        bValue = b.latest_price;
      } else if (sortField === 'symbol') {
        aValue = a.symbol;
        bValue = b.symbol;
        return direction === 'asc' ? aValue.localeCompare(bValue) : bValue.localeCompare(aValue);
      }

      return direction === 'asc' ? aValue - bValue : bValue - aValue;
    });
  };

  // Determine which coins to display based on active tab
  let displayedCoins;
  switch (activeTab) {
    case 'rising':
      displayedCoins = sortCoins(filteredRisingCoins, 'desc');
      break;
    case 'falling':
      displayedCoins = sortCoins(filteredFallingCoins, 'desc', true);
      break;
    default:
      displayedCoins = filteredCoins;
  }

  // Sort the rising and falling coins for the dual-column view will be done in the render function
  // to ensure consistent sorting with the current sortField and sortDirection

  // Helper function to determine current cycle
  const getCurrentCycle = (coin) => {
    // Check which cycle fields are populated (non-zero)
    // Start from the highest cycle and work down
    for (let i = 10; i >= 1; i--) {
      if (coin[`high_price_${i}`] !== 0 || coin[`low_price_${i}`] !== 0) {
        // If we find a populated cycle, the current cycle is the next one
        // But we can't go higher than 10
        return Math.min(i + 1, 10);
      }
    }
    return 1; // Default to cycle 1 if no history yet
  };

  // Function to handle column header clicks for sorting
  const handleSortClick = (field) => {
    if (sortField === field) {
      // If already sorting by this field, toggle direction
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      // If sorting by a new field, set it and default to descending
      setSortField(field);
      setSortDirection('desc');
    }
  };

  return (
    <div className="coin-list">
      <h2>Cryptocurrencies</h2>

      <div className="coin-tabs">
        <button 
          className="tab-button active"
        >
          All Coins ({filteredCoins.length})
        </button>
      </div>

      <div className="search-container">
        <div className="search-input-wrapper">
          <input
            type="text"
            placeholder="Search by symbol..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="search-input"
          />
          {searchTerm && (
            <button 
              className="search-clear-button"
              onClick={() => setSearchTerm('')}
              title="Clear search"
            >
              ×
            </button>
          )}
        </div>
      </div>

      <div className="dual-column-container">
        <div className="column">
          <div className="column-header">Rising</div>
          <div className="list-header">
            <span 
              className={`symbol ${sortField === 'symbol' ? `sorted ${sortDirection}` : ''}`}
              onClick={() => handleSortClick('symbol')}
            >
              Symbol
            </span>
            <span className="cycle">Cycle</span>
            <span 
              className={`price ${sortField === 'price' ? `sorted ${sortDirection}` : ''}`}
              onClick={() => handleSortClick('price')}
            >
              Price (USD)
            </span>
            <span 
              className={`change ${sortField === 'change' ? `sorted ${sortDirection}` : ''}`}
              onClick={() => handleSortClick('change')}
            >
              Change
            </span>
          </div>
          <div className="list-body">
            {filteredRisingCoins.length === 0 ? (
              <p>{searchTerm ? "No rising coins match your search" : "No rising coins"}</p>
            ) : (
              sortCoins(filteredRisingCoins, 'desc').map((coin) => {
                const priceChange = coin.latest_price - coin.initial_price;
                const priceChangePercent = calculatePercentChange(coin.latest_price, coin.initial_price);
                const cycle = getCurrentCycle(coin);

                return (
                  <div
                    key={coin.symbol}
                    className={`coin-item ${selectedCoin && selectedCoin.symbol === coin.symbol ? 'selected' : ''}`}
                    onClick={() => onSelectCoin(coin)}
                  >
                    <span className="symbol">{coin.symbol}</span>
                    <span className="cycle">{cycle > 0 ? cycle : '-'}</span>
                    <span className="price">${coin.latest_price.toFixed(7)}</span>
                    <span className="change positive">
                      +{priceChangePercent.toFixed(2)}%
                    </span>
                  </div>
                );
              })
            )}
          </div>
        </div>
        <div className="column">
          <div className="column-header">Falling</div>
          <div className="list-header">
            <span 
              className={`symbol ${sortField === 'symbol' ? `sorted ${sortDirection}` : ''}`}
              onClick={() => handleSortClick('symbol')}
            >
              Symbol
            </span>
            <span className="cycle">Cycle</span>
            <span 
              className={`price ${sortField === 'price' ? `sorted ${sortDirection}` : ''}`}
              onClick={() => handleSortClick('price')}
            >
              Price (USD)
            </span>
            <span 
              className={`change ${sortField === 'change' ? `sorted ${sortDirection}` : ''}`}
              onClick={() => handleSortClick('change')}
            >
              Change
            </span>
          </div>
          <div className="list-body">
            {filteredFallingCoins.length === 0 ? (
              <p>{searchTerm ? "No falling coins match your search" : "No falling coins"}</p>
            ) : (
              sortCoins(filteredFallingCoins, 'desc', true).map((coin) => {
                const priceChange = coin.latest_price - coin.initial_price;
                const priceChangePercent = calculatePercentChange(coin.latest_price, coin.initial_price);
                const cycle = getCurrentCycle(coin);

                return (
                  <div
                    key={coin.symbol}
                    className={`coin-item ${selectedCoin && selectedCoin.symbol === coin.symbol ? 'selected' : ''}`}
                    onClick={() => onSelectCoin(coin)}
                  >
                    <span className="symbol">{coin.symbol}</span>
                    <span className="cycle">{cycle > 0 ? cycle : '-'}</span>
                    <span className="price">${coin.latest_price.toFixed(7)}</span>
                    <span className="change negative">
                      {priceChangePercent.toFixed(2)}%
                    </span>
                  </div>
                );
              })
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default CoinList;
