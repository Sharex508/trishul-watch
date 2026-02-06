# Coin Price Monitor — Single Technical Flow Diagram

This file contains a single, consolidated tech flow diagram for the entire system (Frontend, Backend, Background Jobs, Data Stores, and the main API/data flows).

Tip: GitHub and many editors render Mermaid automatically. If you don’t see a diagram, use a Mermaid plugin/preview (e.g., VS Code "Markdown Preview Mermaid Support").

```mermaid
flowchart TB
  %% High-level components
  Browser[User Browser]
  FE[React Frontend (port 3000)]
  API[FastAPI Backend (port 8000)]
  DB[(Database\nPostgres (Docker) or SQLite file)]

  Browser -->|HTTP| FE
  FE -->|HTTP REST| API
  API <-->|SQL (driver-aware via _q ?→%s)| DB

  %% Background jobs running inside API process
  subgraph BG[Background Jobs (threads inside API)]
    direction TB
    PM[PriceMonitor\n(app/coin_price_monitor.py)\n• Polls Binance prices\n• Updates coin_monitor]
    CI[CandleIngestor\n(app/ai_pipeline.py)\n• Pulls 1m klines\n• Writes candles]
    FC[FeatureComputer\n(app/ai_pipeline.py)\n• Computes EMA/RSI/MACD/etc.\n• Writes features]
    PD[PatternDiscovery (KMeans)\n(app/ai_pipeline.py)\n• Clusters feature windows\n• Writes pattern_clusters & pattern_assignments]
    RU[RegimeUpdater (heuristic)\n(app/ai_pipeline.py)\n• Labels regime from EMA slope & vol\n• Writes regime_states]
    OI[OrderbookIngestor\n(app/ai_pipeline.py)\n• Snapshots depth & aggregates trades\n• Writes orderbook_snapshots & orderflow]
    TM[TradingManager (paper)\n(app/trading.py)\n• Start/Stop/Reset\n• Logs trades & maintains paper positions]
  end

  API <--> PM
  API <--> CI
  API <--> FC
  API <--> PD
  API <--> RU
  API <--> OI
  API <--> TM

  %% Data stores (tables)
  CM[(coin_monitor)]
  PH[(price_history)]
  C[(candles)]
  F[(features)]
  PC[(pattern_clusters)]
  PA[(pattern_assignments)]
  RS[(regime_states)]
  PE[(pattern_events)]
  OBS[(orderbook_snapshots)]
  OF[(orderflow)]
  TL[(trade_logs)]
  PP[(paper_positions)]
  PF[(paper_portfolio)]

  %% Background jobs <-> tables
  PM -->|updates prices, cycles| CM
  CI -->|writes| C
  FC -->|reads| C
  FC -->|writes indicators| F
  PD -->|reads windows| F
  PD -->|writes clusters| PC
  PD -->|writes assignments| PA
  RU -->|writes| RS
  OI -->|writes| OBS
  OI -->|writes| OF
  TM -->|reads prices| CM
  TM -->|writes\n(completed trades)| TL
  TM -->|writes/updates| PP
  TM -->|writes/updates| PF

  %% API endpoints (selected) used by Frontend
  FE -.->|GET /api/coin-monitors| API
  FE -.->|GET /api/market/candles/latest| API
  FE -.->|GET /api/market/features/latest| API
  FE -.->|GET /api/patterns| API
  FE -.->|GET /api/patterns/active| API
  FE -.->|POST /api/patterns/discover| API
  FE -.->|GET /api/ai/patterns/recent| API
  FE -.->|GET /api/ai/regime/latest| API
  FE -.->|GET /api/trading/status| API
  FE -.->|POST /api/trading/start| API
  FE -.->|POST /api/trading/stop| API
  FE -.->|POST /api/trading/reset| API
  FE -.->|GET /api/trade-logs| API
  FE -.->|GET /api/predictions| API

  %% API <-> tables (representative)
  API -->|SELECT/INSERT/UPDATE| CM
  API -->|SELECT| C
  API -->|SELECT| F
  API -->|SELECT| PC
  API -->|SELECT latest| PA
  API -->|SELECT| RS
  API -->|SELECT| PE
  API -->|SELECT| OBS
  API -->|SELECT| OF
  API -->|SELECT/INSERT| TL
  API -->|SELECT/UPDATE| PP
  API -->|SELECT/UPDATE| PF

  %% Startup & configuration context
  subgraph BOOT[Startup & Config]
    direction TB
    S1[FastAPI on_event("startup")\n(app/main.py)\n• start_price_monitor()\n• start_ai_background_jobs()]
    S2[Env & Compose config\n• DB_HOST/USER/PASS/NAME/PORT\n• API_HOST/API_PORT\n• AI_TIMEFRAMES\n• PATTERN_* cadence/params\n• ORDERBOOK_* cadence/levels\n• PAPER_* risk/caps]
  end

  S1 --> PM
  S1 --> CI
  S1 --> FC
  S1 --> PD
  S1 --> RU
  S1 --> OI
  S2 -.-> API

  %% Docker compose context (summary)
  subgraph DC[Docker Compose]
    direction LR
    PG[postgres:13\nexposed 5433->5432]
    APIC[api container\nuvicorn app.main:app\nexposed 8000]
    FEC[frontend container\nreact-scripts start\nexposed 3000]
  end

  APIC --- API
  FEC --- FE
  PG --- DB

  classDef store fill:#0b1020,stroke:#1f2a44,color:#a7c7ff
  class CM,PH,C,F,PC,PA,RS,PE,OBS,OF,TL,PP,PF store
```

## Notes
- The same Python code works with Postgres and SQLite by translating SQL placeholders using `_q(sql, is_postgres)`.
- Patterns and Predictions become non-empty after candles and features accumulate. Use `POST /api/patterns/discover?symbol=BTCUSDT` to seed quickly.
- Paper trading is demo-safe with configurable caps and simple exits; trades are visible in Trade Logs.
