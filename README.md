# Film Discovery and Ultra-Reliable Parser

A comprehensive Python system for discovering and parsing film information from ekinobilet.fond-kino.ru with ultra-reliable parsing capabilities and browser automation.

## 🎯 Features

- **Automated Film Discovery**: Browser automation to discover films by date range
- **Reliable Parsing**: design with retry mechanisms
- **Configurable Parameters**: Easy-to-modify date ranges and parser settings
- **Comprehensive Output**: CSV export with detailed film metadata and parsing timestamps
- **High Performance**: Concurrent parsing with adaptive rate limiting

## 📋 Requirements

### Python Dependencies
```bash
pip install -r requirements.txt
```

Required packages:
- `aiohttp >= 3.8.0` - Async HTTP client for concurrent requests
- `beautifulsoup4 >= 4.9.3` - HTML parsing
- `lxml >= 4.6.3` - XML/HTML parser
- `playwright >= 1.40.0` - Browser automation
- `pandas >= 1.3.0` - Data handling and CSV export
- `asyncio` - Async programming (built-in)

### Browser Installation
```bash
playwright install chromium
```

## 🚀 Quick Start

### 1. Basic Usage
```bash
# Run with default settings
python film_discovery_and_parse.py
```

### 2. Configure Date Range and Parser Settings

Edit the configuration section in `film_discovery_and_parse.py`:

```python
# DATE RANGE SETTINGS
START_DATE = "01 янв 2025"      # Change start date here
END_DATE = "31 авг 2025"        # Change end date here

# PARSER SETTINGS
BATCH_SIZE = 20                 # URLs per batch (smaller = more reliable)
BATCH_PAUSE = 1.0              # Pause between batches (seconds)
MAX_CONCURRENT = 10             # Max concurrent requests (lower = more reliable)
MAX_RETRIES = 3                # Max retry attempts per URL
```

## ⚙️ Configuration Options

### Date Range Settings
- **START_DATE**: Start date for film discovery (format: "01 янв 2025")
- **END_DATE**: End date for film discovery (format: "31 авг 2025")

### Parser Performance Settings

**Fast Processing (Higher Risk):**
```python
BATCH_SIZE = 25
BATCH_PAUSE = 0.5
MAX_CONCURRENT = 15
MAX_RETRIES = 2
```

**Balanced (Recommended):**
```python
BATCH_SIZE = 20
BATCH_PAUSE = 1.0
MAX_CONCURRENT = 10
MAX_RETRIES = 3
```

**Maximum Reliability (Slower):**
```python
BATCH_SIZE = 10
BATCH_PAUSE = 2.0
MAX_CONCURRENT = 5
MAX_RETRIES = 5
```

## 📊 Output

### CSV File Structure
The parser generates timestamped CSV files with the following columns:

- `url` - Film detail page URL
- `title_name` - Film title
- `total_fees` - Total box office revenue
- `presales_fees` - Presales revenue
- `premiere_day_fees` - Premiere day revenue
- `first_weekend_fees` - First weekend revenue
- `second_weekend_fees` - Second weekend revenue
- `country` - Production country
- `start_date` - Release date
- `year` - Release year
- `age_restriction` - Age rating
- `error` - Error message (if parsing failed)
- `parse_time` - Time taken to parse (seconds)
- `attempt_count` - Number of parsing attempts
- `batch_number` - Processing batch number
- `parsing_date` - Date when data was collected
- `parsing_datetime` - Exact timestamp of data collection

### Report File
A detailed text report is also generated with:
- Parsing statistics and success rates
- Performance metrics
- Failed URLs (if any)
- Retry analysis

## 🔧 Usage Examples

### Example 1: Parse Recent Films
```python
# Edit film_discovery_and_parse.py
START_DATE = "01 дек 2024"
END_DATE = "31 дек 2024"
BATCH_SIZE = 15
MAX_CONCURRENT = 8
```

### Example 2: Parse Full Year (Large Dataset)
```python
# Edit film_discovery_and_parse.py
START_DATE = "01 янв 2025"
END_DATE = "31 дек 2025"
BATCH_SIZE = 10          # Smaller batches for reliability
BATCH_PAUSE = 1.5        # Longer pauses
MAX_CONCURRENT = 5       # Lower concurrency
MAX_RETRIES = 4          # More retries
```

## 📈 Performance

### Typical Results
- **Discovery Phase**: 10-60 seconds (depending on date range)
- **Parsing Phase**: 2-8 URLs per second (depending on settings)
- **Success Rate**: 95-99% (with proper configuration)
- **Memory Usage**: Optimized for large datasets

## 🛠️ Troubleshooting

### Common Issues

**High failure rates:**
- Reduce `BATCH_SIZE` and `MAX_CONCURRENT`
- Increase `BATCH_PAUSE` and `MAX_RETRIES`

**Slow performance:**
- Increase `BATCH_SIZE` and `MAX_CONCURRENT`
- Reduce `BATCH_PAUSE`

**Browser automation fails:**
```bash
# Reinstall playwright browsers
playwright install chromium
```

**Memory issues with large datasets:**
- Use smaller date ranges
- Reduce `BATCH_SIZE`

## 📁 Project Structure

- `film_discovery_and_parse.py` - Main script with configurable settings
- `ultra_reliable_parser.py` - Core parsing engine
- `requirements.txt` - Python dependencies
- `film_discovery_results_*.csv` - Generated results files
- `film_discovery_report_*.txt` - Generated report files

## 📄 Example Output

### CSV Results Sample:
```
title_name,total_fees,country,start_date,year,success_rate
"Фильм 1","1,234,567 руб","Россия","01.08.2025","2025",99.2%
"Фильм 2","987,654 руб","США","15.08.2025","2025",99.2%
```

### Report Sample:
```
Total URLs: 150
Successful: 149 (99.3%)
Failed: 1 (0.7%)
Average parse time: 2.1s
```

## ⚖️ Responsible Usage

**Please use this tool ethically:**
- Respect the target website's terms of service
- Use reasonable delays between requests (default settings are server-friendly)
- Don't overwhelm servers with excessive concurrent requests
- Consider reaching out to site owners for permission if scraping large datasets
- This tool is intended for research, journalism, and educational purposes

**The parser includes built-in protections:**
- Adaptive rate limiting based on server response
- Respectful delays between batches
- Browser-like request headers
- Automatic retry with exponential backoff

