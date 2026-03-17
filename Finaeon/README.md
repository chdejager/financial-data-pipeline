# Finaeon Symbol Discovery and Time Series Scraper

Production-ready script to discover financial symbols via the [Finaeon API](https://finaeon.com/) and batch scrape their time series data.

## Features

- Symbol discovery with fixed or adaptive (paging-aware) strategies
- Batch scraping of time series data
- Automatic deduplication (skips already-scraped symbols)
- Token refresh on expiration
- Checkpointing during long discovery runs
- Detailed progress logging and manifests

## Configuration

### Output Directories

By default, data is saved to `~/finaeon_data/`. You can customize this using environment variables:

```bash
export FINAEON_SERIES_DIR="/path/to/your/series/data"
export FINAEON_MANIFEST_DIR="/path/to/your/manifests"
```

Or set them for a single run:

```bash
FINAEON_SERIES_DIR=./data/series FINAEON_MANIFEST_DIR=./data/manifests python finaeon_discovery_scraper.py
```

**Default locations:**
- Series data: `~/finaeon_data/series/`
- Manifests: `~/finaeon_data/manifest/`

### API Credentials

The script uses trial credentials by default (`tryapi@finaeon.com`). For production:

1. Update `GFD_USERNAME` and `GFD_PASSWORD` in the script, or
2. Set environment variables:
   ```bash
   export GFD_USERNAME="your@email.com"
   export GFD_PASSWORD="your_password"
   ```

### API URL (Optional)

If using a custom API endpoint:

```bash
export GFD_API_URL="https://custom-api.finaeon.com/"
```

## Usage

### Full Pipeline (Discover + Scrape)

```bash
python finaeon_discovery_scraper.py
```

### Discovery Only

```bash
python finaeon_discovery_scraper.py --discovery-only
```

### Fixed vs. Adaptive Discovery

**Fixed strategy** (faster, less thorough):
```bash
python finaeon_discovery_scraper.py --discovery-strategy fixed --max-pages 5
```

**Adaptive strategy** (comprehensive, avoids deep pagination):
```bash
python finaeon_discovery_scraper.py --discovery-strategy adaptive --page-threshold 5
```

### With Date Range

```bash
python finaeon_discovery_scraper.py --start-date "01/01/2020" --end-date "12/31/2023"
```

### Load Pre-Computed Discovery

If you've already run discovery, reuse it:

```bash
python finaeon_discovery_scraper.py --load-discovery ./data/manifests/discovery_20260315_120000.json
```

### Diagnostics

Probe pagination behavior for a specific prefix:
```bash
python finaeon_discovery_scraper.py --probe-prefix "A" --probe-pages 25
```

Test a single symbol:
```bash
python finaeon_discovery_scraper.py --test-symbol "AAPL"
```

## Output Structure

```
~/finaeon_data/
├── series/
│   └── discovered_YYYYMMDD/
│       ├── series__SYMBOL__monthly__full.json
│       └── batch_config.json
└── manifest/
    ├── discovery_YYYYMMDD_HHMMSS.json
    ├── batch_discovered_YYYYMMDD_HHMMSS.json
    └── pipeline_final_YYYYMMDD_HHMMSS.json
```

## Requirements

- Python 3.7+
- `requests`
- `pandas`

Install dependencies:
```bash
pip install requests pandas
```

