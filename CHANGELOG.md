# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1] - 2025-09-04

### Changed
- **Data Type Improvements**: Fee columns now converted to integers (removing currency symbols, spaces, commas)
- **Date Format Standardization**: start_date now formatted as YYYY-MM-DD (e.g., "28 авг. 2025" → "2025-08-28")
- **Enhanced Russian Date Parsing**: Added support for abbreviated month names with periods (авг., дек., etc.)

### Technical
- Added `_convert_fees_to_int()` function for proper numeric data types
- Added `_convert_date_to_datetime()` function with comprehensive Russian month mapping
- Data conversion happens during CSV export for clean output format

## [0.1.0] - 2025-09-04

### Added
- Initial release of film discovery and parsing system
- Browser automation for film URL discovery using Playwright
- Ultra-reliable parser with adaptive rate limiting
- Batch processing with configurable parameters
- Comprehensive error handling and retry mechanisms
- CSV export with detailed film metadata
- Performance reporting and statistics
- MIT license
- Complete documentation and usage examples

### Features
- **Film Discovery**: Automated browser-based discovery by date range
- **Reliable Parsing**: 95-99% success rate with built-in retries
- **Configurable Settings**: Customizable batch sizes, delays, and concurrency
- **Server-Friendly**: Adaptive rate limiting and respectful request patterns
- **Comprehensive Output**: Timestamped CSV files with detailed metadata
- **Performance Monitoring**: Real-time progress tracking and detailed reports

### Technical Details
- Python 3.13+ support
- Async/await architecture for high performance
- Playwright for browser automation
- BeautifulSoup for HTML parsing
- Pandas for data export
- Comprehensive logging and error handling
