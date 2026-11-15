# Bybit BTC Options to NautilusTrader Parquet Catalog

This project converts Bybit BTC options market and execution data into a production-ready **NautilusTrader Parquet Data Catalog** along with normalized CSV inspection files.

---

## Features

- **Master Dataset Support**: Directly ingests tagged BTC market data, including 1-minute BTCUSDT candles, option executions, and timestamped option quote/ticker snapshots.
- **Native Instrument Specifications**: Automatically queries Bybit's public API (`/v5/market/instruments-info`) to build and write native `CryptoPerpetual` and `CryptoOption` definitions into the catalog.
- **Smart Fallback Mechanism**: If historical expired options are no longer listed in Bybit's active registry, it dynamically checks listed BTC options for uniform tick/lot rules and validates fill increments before generating standard instrument definitions.
- **Typed Nautilus Data**: Writes `Bar`, option `TradeTick`, `QuoteTick`, and (when the feed includes timestamped Greeks) `OptionGreeks` records in NautilusTrader's Parquet catalog format.
- **Normalized CSV Output**: Simultaneously exports clean, readable CSV files for verification and external analysis.

---

## Project Structure

```text
├── convert_to_catalog.py          # Main NautilusTrader catalog conversion pipeline
├── bybit_master_market_data.csv   # Combined underlying candles & option execution trades
├── requirements.txt               # Pinned dependencies (Python 3.11 / 3.12)
└── README.md                      # Documentation and run steps
```

---

## Setup Instructions

### 1. Prerequisites
- **Python 3.11 or 3.12** (NautilusTrader requires Python 3.11/3.12 for pre-compiled binary wheels).

### 2. Create Virtual Environment
Open PowerShell or your command prompt in this directory:

```powershell
# Create a virtual environment named .venv
python -m venv .venv
```
*(If you have multiple Python versions installed, specify Python 3.12: `py -3.12 -m venv .venv`)*

### 3. Install Dependencies
```powershell
# Upgrade pip
.\.venv\Scripts\python.exe -m pip install --upgrade pip

# Install required packages
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

---

## How to Run

### Method 1: Direct Execution (Recommended — No Activation Needed)
Run the script passing the master market data file:

```powershell
.\.venv\Scripts\python.exe convert_to_catalog.py --overwrite bybit_master_market_data.csv
```

### Method 2: Activated Environment
Activate the virtual environment first, then run:

```powershell
# If script execution is restricted on Windows PowerShell:
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Activate environment
.\.venv\Scripts\Activate.ps1

# Run converter
python convert_to_catalog.py --overwrite bybit_master_market_data.csv
```

### Method 3: Interactive File Picker
If you run without arguments, a graphical file picker opens to let you select a tagged master CSV or individual candle, option execution, and timestamped option quote CSVs. Untimestamped metadata snapshots are not treated as historical quotes.

```powershell
.\.venv\Scripts\python.exe convert_to_catalog.py --overwrite
```

---

## Outputs

When the conversion finishes, the only output folders are:

1. **`bybit_nautilus_catalog/`**:
   - `data/` — Nautilus Parquet catalog containing:
     - `bar/` (`BTCUSDT-LINEAR.BYBIT-1-MINUTE-LAST-EXTERNAL`)
     - `trade_tick/` (`BTC-*-OPTION.BYBIT`)
2. **`bybit_normalized/`**:
   - `btc_underlying_bars.csv` — Normalized BTC 1m OHLCV bars.
   - `btc_option_execution_ticks.csv` — Normalized option execution trade ticks.
   - `btc_option_quotes.csv` — Normalized timestamped best bid/ask quotes, when supplied.
   - `btc_option_greeks.csv` — Normalized timestamped Greeks, when supplied with quotes.

The converter writes native catalog files under `bybit_nautilus_catalog/data/` and the
human-readable normalized CSV views under `bybit_normalized/`. It removes stale
normalized views from earlier runs when that record type is absent from the current input.
Raw source files are inputs and are not copied into the Nautilus catalog.

---

## Notes
- **Network Access**: An active internet connection is used during execution to fetch real contract specifications from Bybit's public API.
- **Data Integrity**: Orders, positions, and closed PnL records are preserved in source format; only actual market trades and candle ticks are written to the Nautilus catalog.
