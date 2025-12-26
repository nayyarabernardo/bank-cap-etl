# Bank Capitalization ETL Pipeline

## Overview

A robust ETL (Extract, Transform, Load) pipeline for extracting banking market capitalization data, converting currencies, and generating analytical outputs. This system automates the collection of financial data from multiple sources and provides currency conversion capabilities for cross-border financial analysis.

## Architecture

```
bank-cap-etl/
├── src/
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── api_extractor.py      # Exchange rate data extraction
│   │   └── web_extractor.py      # Banking data extraction from Wikipedia
│   ├── transform/
│   │   ├── __init__.py
│   │   └── market_cap_transformer.py  # Currency conversion logic
│   ├── load/
│   │   ├── __init__.py
│   │   └── file_loader.py        # Incremental data loading
│   └── main.py                   # Main pipeline orchestrator
├── data/
│   ├── raw/
│   │   ├── exchange_rates/       # Raw JSON exchange rate data
│   │   └── banks/               # Raw JSON banking data
│   ├── processed/
│   │   ├── outputs/             # Processed CSV files
│   │   └── reports/             # Analysis reports
│   └── consolidated/            # Historical consolidated data
├── config/
│   └── settings.py              # Configuration and constants
├── logs/                        # Execution logs
├── tests/                       # Test suite
├── .env                         # Environment variables
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

## Technical Stack

- **Python 3.8+**
- **Pandas** - Data manipulation and transformation
- **Requests** - HTTP API calls
- **Logging** - Comprehensive execution tracking
- **JSON/CSV** - Data serialization formats

## Installation

### Prerequisites
- Python 3.8 or higher
- Git
- API key from [ExchangeRate-API](https://apilayer.com/marketplace/exchangerates_data-api)

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/bank-cap-etl.git
cd bank-cap-etl
```

2. **Create virtual environment**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment variables**
```bash
cp .env.example .env
# Edit .env and add your API key
# EXCHANGE_API_KEY=your_api_key_here
```

## Configuration

### Environment Variables
Create a `.env` file in the project root:

```env
EXCHANGE_API_KEY=your_apilayer_api_key_here
```

### Project Settings
Modify `config/settings.py` for custom configurations:

```python
# API Configuration
EXCHANGE_API_URL = "https://api.apilayer.com/exchangerates_data/latest"
BANKS_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_largest_banks"

# File Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
```

## Pipeline Components

### 1. Extraction Module

#### API Extractor (`src/extract/api_extractor.py`)
- Fetches real-time exchange rates from ExchangeRate-API
- Supports multiple base currencies
- Saves raw JSON with metadata and timestamp
- Automatic retry and error handling

#### Web Extractor (`src/extract/web_extractor.py`)
- Scrapes banking data from Wikipedia tables
- Extracts market capitalization in USD billions
- Handles HTTP errors and timeouts
- Preserves extraction metadata

### 2. Transformation Module (`src/transform/market_cap_transformer.py`)
- Currency conversion using latest exchange rates
- Automatic column detection and cleaning
- Data validation and error handling
- Metadata enrichment (timestamps, conversion rates)
- Supports multiple currency pairs (USD→GBP, USD→EUR, etc.)

### 3. Loading Module (`src/load/file_loader.py`)
- Incremental data loading with deduplication
- Multiple output formats (CSV, consolidated, timestamped)
- Historical tracking with JSON metadata
- File versioning and retention management

## Usage

### Basic Execution

Run the complete ETL pipeline with default settings (USD to GBP):

```bash
python -m src.main
```

### Custom Currency Conversion

Modify `src/main.py` to use different currency pairs:

```python
if __name__ == "__main__":
    # Convert USD to Euro
    run_etl_pipeline("USD", "EUR")
    
    # Convert USD to Brazilian Real
    run_etl_pipeline("USD", "BRL")
    
    # Convert Euro to British Pound
    run_etl_pipeline("EUR", "GBP")
```

### Programmatic Usage

```python
from src.main import run_etl_pipeline

# Execute pipeline
result_df = run_etl_pipeline(
    base_currency="USD",
    target_currency="EUR"
)

# Access results
print(f"Processed {len(result_df)} banks")
print(f"Exchange rate: {result_df['_exchange_rate'].iloc[0]}")
```

## Output Structure

The pipeline generates multiple output files for different use cases:

### Data Files
```
data/processed/outputs/
├── bank_market_cap_eur_2024-01-15.csv      # Daily consolidated file
├── bank_market_cap_eur_20240115_143022.csv # Timestamped snapshot
├── bank_market_cap_eur_consolidated.csv    # Historical consolidated
└── load_history.json                       # Execution metadata
```

### File Descriptions

1. **Daily Consolidated** (`_YYYY-MM-DD.csv`)
   - Contains all data for the specific day
   - Updated incrementally with each run
   - Used for daily analysis

2. **Timestamped Snapshot** (`_YYYYMMDD_HHMMSS.csv`)
   - Exact state of data at execution time
   - Used for debugging and point-in-time analysis
   - Retention: 7 days (configurable)

3. **Historical Consolidated** (`_consolidated.csv`)
   - Complete historical dataset
   - Updated with latest data, deduplicated
   - Used for trend analysis

4. **Execution Metadata** (`load_history.json`)
   - JSON log of all pipeline executions
   - Includes timestamps, row counts, file names
   - Used for auditing and monitoring

## Data Schema

### Raw Banking Data
```json
{
  "Rank": 1,
  "Bank name": "Industrial and Commercial Bank of China",
  "Total assets (2025) (US$ billion)": 6688.74,
  "_extracted_at": "2024-01-15 14:30:22.123456"
}
```

### Processed Output
```csv
Rank,Bank name,Total assets (2025) (US$ billion),_extracted_at,
assets_usd_billion,assets_gbp_billion,_transformed_at,_exchange_rate,
_exchange_from,_exchange_to,_exchange_date
```

### Key Columns
- **Rank**: Bank ranking by assets
- **Bank name**: Financial institution name
- **Total assets**: Assets in USD billions (original)
- **_extracted_at**: Data extraction timestamp
- **assets_usd_billion**: Cleaned USD assets
- **assets_{currency}_billion**: Converted assets
- **_exchange_rate**: Conversion rate used
- **_exchange_from/to**: Source and target currencies

## Testing

Run the test suite to verify pipeline components:

```bash
# Run all tests
python -m pytest tests/

# Run specific test module
python -m pytest tests/test_multiple_currencies.py -v

# Run with coverage report
python -m pytest --cov=src tests/
```

### Test Structure
```
tests/
└── test_multiple_currencies.py  # End-to-end pipeline tests
```

## Monitoring & Logging

### Log Files
Logs are stored in the `logs/` directory with daily rotation:
```
logs/
├── etl_20240115.log    # Today's execution log
├── etl_20240114.log    # Previous day's log
└── error_20240115.log  # Error-specific log
```

### Log Levels
- **INFO**: Pipeline execution steps
- **DEBUG**: Detailed data processing
- **WARNING**: Non-critical issues
- **ERROR**: Processing failures
- **CRITICAL**: System failures

## Scheduling

For automated daily execution, use system schedulers:

### Linux/Mac (Cron)
```bash
# Edit crontab
crontab -e

# Add line for daily 9 AM execution
0 9 * * * cd /path/to/bank-cap-etl && .venv/bin/python -m src.main
```

### Windows (Task Scheduler)
Create a scheduled task that runs:
```
cmd /c "cd C:\path\to\bank-cap-etl && .venv\Scripts\python.exe -m src.main"
```

## Error Handling

The pipeline includes comprehensive error handling:

### Retry Logic
- API call retries with exponential backoff
- Network timeout handling
- Rate limit management

### Data Validation
- Schema validation for extracted data
- Null value detection and handling
- Currency code validation

### Fallback Mechanisms
- Local cache for exchange rates
- Graceful degradation when APIs are unavailable
- Data quality warnings without pipeline failure

## Performance

### Benchmarks
- **Extraction**: ~2-3 seconds (API + web scraping)
- **Transformation**: <1 second for 100 records
- **Full Pipeline**: ~5 seconds end-to-end

### Optimization Features
- Incremental loading to avoid reprocessing
- Parallel API calls where possible
- Efficient memory usage with Pandas chunks
- Disk I/O optimization

## Future Enhancements

### Phase 2: Data Visualization
```python
# Planned visualization module
src/visualization/
├── chart_generator.py    # Matplotlib/Plotly charts
├── dashboard_builder.py  # Interactive dashboards
└── report_generator.py   # PDF/HTML reports
```


### Visualization Types
- Trend analysis of bank assets over time
- Currency exchange rate trends
- Geographical distribution of banking assets
- Top-N bank comparisons
- Correlation analysis between currencies

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add new feature'`)
4. Push to branch (`git push origin feature/improvement`)
5. Create Pull Request

### Development Guidelines
- Follow PEP 8 coding standards
- Write tests for new functionality
- Update documentation for changes
- Use type hints for better code clarity

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [ExchangeRate-API](https://apilayer.com/marketplace/exchangerates_data-api) for currency data
- Wikipedia for banking information
- Pandas community for data manipulation tools


