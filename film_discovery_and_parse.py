#!/usr/bin/env python3
"""
Film Discovery and Ultra-Reliable Parsing

DESCRIPTION:
    Simple script that combines film URL discovery with ultra-reliable parsing.
    First collects URLs using browser automation, then parses them with ultra_reliable_parser.

WORKFLOW:
    1. Use search_films_with_browser() to collect film URLs
    2. Feed URLs to ultra_reliable_parser for parsing
    3. Save results to CSV

USAGE:
    python film_discovery_and_parse.py
"""

import asyncio
import time
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple
from playwright.async_api import async_playwright
from page_parser import UltraReliableParser
import re


def _convert_fees_to_int(fee_str):
    """Convert fee string to integer (remove spaces, commas, currency)."""
    if pd.isna(fee_str) or fee_str is None or fee_str == '':
        return None
    
    # Remove all non-digit characters except spaces and commas
    clean_str = re.sub(r'[^\d\s,]', '', str(fee_str))
    # Remove spaces and commas
    clean_str = re.sub(r'[\s,]', '', clean_str)
    
    try:
        return int(clean_str) if clean_str else None
    except ValueError:
        return None


def _convert_date_to_datetime(date_str):
    """Convert Russian date string to YYYY-MM-DD format."""
    if pd.isna(date_str) or date_str is None or date_str == '':
        return None
    
    # Russian month mapping (including abbreviated forms with periods)
    month_map = {
        'янв': '01', 'янв.': '01', 'января': '01',
        'февр': '02', 'февр.': '02', 'февраля': '02',
        'мар': '03', 'мар.': '03', 'марта': '03',
        'апр': '04', 'апр.': '04', 'апреля': '04',
        'май': '05', 'май.': '05', 'мая': '05',
        'июн': '06', 'июн.': '06', 'июня': '06',
        'июл': '07', 'июл.': '07', 'июля': '07',
        'авг': '08', 'авг.': '08', 'августа': '08',
        'сент': '09', 'сент.': '09', 'сентября': '09',
        'окт': '10', 'окт.': '10', 'октября': '10',
        'нояб': '11', 'нояб.': '11', 'ноября': '11',
        'дек': '12', 'дек.': '12', 'декабря': '12'
    }
    
    try:
        # Try to match patterns like "28 авг. 2025", "01 авг 2025" or "1 августа 2025"
        pattern = r'(\d{1,2})\s+(\w+\.?)\s+(\d{4})'
        match = re.search(pattern, str(date_str))
        
        if match:
            day = match.group(1).zfill(2)  # Pad with zero if needed
            month_name = match.group(2).lower()
            year = match.group(3)
            
            # Find month number
            month = month_map.get(month_name)
            
            if month:
                return f"{year}-{month}-{day}"
    except:
        pass
    
    return date_str  # Return original if conversion fails


def _extract_age_from_restriction(age_restriction_str):
    """Extract age number from age restriction string."""
    if pd.isna(age_restriction_str) or age_restriction_str is None or age_restriction_str == '':
        return None
    
    try:
        # Look for patterns like "0+", "6+", "12+", "16+", "18+"
        pattern = r'(\d+)\+'
        match = re.search(pattern, str(age_restriction_str))
        
        if match:
            return int(match.group(1))
    except:
        pass
    
    return None


async def search_films_with_browser(start_date: str = "01 авг 2025", end_date: str = "25 авг 2025") -> List[str]:
    """
    Browser automation to search for films and collect their URLs.
    Returns only the URLs - no parsing here.
    Format of start and end date should be like this, otherwise it will not work
    
    Args:
        start_date: Start date in format "01 авг 2025"
        end_date: End date in format "25 авг 2025"
    """
    
    film_urls = []
    start_time = time.time()

    try:
        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            page.set_default_timeout(10000)

            print("Opening films page...")
            await page.goto("https://ekinobilet.fond-kino.ru/films/")

            print(f'Inputting start date: {start_date}')
            await page.click('input[name="periodStart"]')
            await page.type('input[name="periodStart"]', start_date)
            await asyncio.sleep(2)
            
            print(f'Inputting end date: {end_date}')
            await page.click('input[name="periodEnd"]')
            await page.type('input[name="periodEnd"]', end_date)
            await asyncio.sleep(2)

            # Handle loading more results
            print("Loading all films...")
            while True:
                button = page.locator('a[href*="#load"]')
                try:
                    if await button.is_enabled() and await button.is_visible():
                        await button.click()
                        print("Clicked load button, waiting for results...")
                        await asyncio.sleep(1)
                    else:
                        print("No more load buttons, finished loading")
                        break
                except Exception:
                    break

            # Extract film URLs
            print("Extracting film URLs...")
            articles = await page.query_selector_all('article')
            
            for article in articles:
                links = await article.query_selector_all('a[href*="/films/detail/"]')
                for link in links:
                    href = await link.get_attribute('href')
                    if href:
                        if href.startswith('/'):
                            full_url = f"https://ekinobilet.fond-kino.ru{href}"
                        else:
                            full_url = href
                        
                        if full_url not in film_urls:
                            title = await link.inner_text()
                            title = title.strip()
                            film_urls.append(full_url)

            await browser.close()
            
    except Exception as e:
        print(f"Browser automation error: {e}")
    
    elapsed_time = time.time() - start_time
    print(f"\nURL Discovery Complete:")
    print(f"Found {len(film_urls)} films in {elapsed_time:.2f} seconds")
    
    return film_urls


async def parse_films_with_ultra_reliable(film_urls: List[str], 
                                         batch_size: int = 12,
                                         batch_pause: float = 1.0,
                                         max_concurrent: int = 6,
                                         max_retries: int = 3):
    """
    Parse film URLs using the ultra-reliable parser.
    
    Args:
        film_urls: List of URLs to parse
        batch_size: Number of URLs per batch
        batch_pause: Pause between batches in seconds
        max_concurrent: Maximum concurrent requests
        max_retries: Maximum retry attempts per URL
    """
    
    if not film_urls:
        print("No URLs to parse!")
        return []
    
    print(f"\nStarting parsing of {len(film_urls)} URLs...")
    
    # Configure ultra-reliable parser with provided options
    async with UltraReliableParser(
        batch_size=batch_size,
        batch_pause=batch_pause,
        max_concurrent=max_concurrent,
        max_retries=max_retries
    ) as parser:
        
        # Progress callback
        async def progress_callback(completed, total, batch_results):
            successful_in_batch = sum(1 for r in batch_results if r.is_successful())
            print(f"Progress: {completed}/{total} completed | Latest batch: {successful_in_batch}/{len(batch_results)} successful")
        
        # Parse all URLs
        results = await parser.parse_urls_in_batches(film_urls, progress_callback)
        
        # Generate report
        report = parser.generate_report(results)
        
        return results, report


def save_results(results, report):
    """Save results and report to files."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    current_date = datetime.now().strftime("%Y-%m-%d")
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save results to CSV
    csv_filename = f"film_discovery_results_{timestamp}.csv"
    results_df = pd.DataFrame([r.to_dict() for r in results])
    
    # Convert fee columns from strings to integers
    fee_columns = ['total_fees', 'presales_fees', 'premiere_day_fees', 'first_weekend_fees', 'second_weekend_fees']
    for col in fee_columns:
        if col in results_df.columns:
            results_df[col] = results_df[col].apply(_convert_fees_to_int)
    
    # Convert start_date to datetime format (YYYY-MM-DD)
    if 'start_date' in results_df.columns:
        results_df['start_date'] = results_df['start_date'].apply(_convert_date_to_datetime)
    
    # Extract age number from age_restriction and create new column
    if 'age_restriction' in results_df.columns:
        results_df['age'] = results_df['age_restriction'].apply(_extract_age_from_restriction)
    
    # Add current date columns
    results_df['parsing_date'] = current_date
    results_df['parsing_datetime'] = current_datetime
    
    results_df.to_csv(csv_filename, index=False)
    
    # Save report to file
    report_filename = f"film_discovery_report_{timestamp}.txt"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write("FILM DISCOVERY AND PARSING REPORT\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"Summary:\n")
        f.write(f"  Total URLs: {report['summary']['total_urls']}\n")
        f.write(f"  Successful: {report['summary']['successful']} ({report['summary']['success_rate']:.1f}%)\n")
        f.write(f"  Failed: {report['summary']['failed']}\n")
        f.write(f"  Retry rate: {report['summary']['retry_rate']:.1f}%\n")
        f.write(f"  Average attempts: {report['summary']['average_attempts']:.1f}\n\n")
        
        f.write(f"Performance:\n")
        f.write(f"  Total time: {report['performance']['total_time']:.1f}s\n")
        f.write(f"  URLs per second: {report['performance']['urls_per_second']:.2f}\n")
        f.write(f"  Average parse time: {report['performance']['average_parse_time']:.2f}s\n\n")
        
        if report['failures']['failed_urls']:
            f.write(f"Failed URLs:\n")
            for url in report['failures']['failed_urls']:
                f.write(f"  - {url}\n")
    
    print(f"\nResults saved to: {csv_filename}")
    print(f"Report saved to: {report_filename}")
    
    return csv_filename, report_filename


async def main(start_date: str = "01 авг 2025", 
               end_date: str = "25 авг 2025",
               batch_size: int = 12,
               batch_pause: float = 1.0,
               max_concurrent: int = 6,
               max_retries: int = 3):
    """
    Main function: Discover URLs, then parse them.
    
    Args:
        start_date: Start date for film discovery
        end_date: End date for film discovery
        batch_size: Number of URLs per batch for parsing
        batch_pause: Pause between batches in seconds
        max_concurrent: Maximum concurrent requests
        max_retries: Maximum retry attempts per URL
    """
    
    print("🎬 FILM DISCOVERY AND  PARSING")
    print("=" * 60)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Parser config: batch_size={batch_size}, max_concurrent={max_concurrent}, retries={max_retries}")
    
    # Step 1: Discover film URLs using browser automation
    print("\nSTEP 1: Discovering film URLs...")
    film_urls = await search_films_with_browser(start_date, end_date)
    
    if not film_urls:
        print("❌ No URLs discovered. Exiting.")
        return
    
    # Step 2: Parse URLs with ultra-reliable parser
    print(f"\nSTEP 2: Parsing {len(film_urls)} URLs ...")
    results, report = await parse_films_with_ultra_reliable(
        film_urls, batch_size, batch_pause, max_concurrent, max_retries
    )
    
    # Step 3: Save results
    print(f"\nSTEP 3: Saving results...")
    csv_file, report_file = save_results(results, report)
    
    # Print summary
    successful = sum(1 for r in results if r.is_successful())
    failed = len(results) - successful
    
    print(f"\n✅ FINAL RESULTS:")
    print(f"   URLs discovered: {len(film_urls)}")
    print(f"   URLs parsed: {len(results)}")
    print(f"   Successful parses: {successful}")
    print(f"   Failed parses: {failed}")
    print(f"   Success rate: {successful/len(results)*100:.1f}%")
    
    # Show sample successful results
    successful_results = [r for r in results if r.is_successful()]
    if successful_results:
        print(f"\n🎬 SAMPLE FILMS (first 5):")
        for i, result in enumerate(successful_results[:5], 1):
            print(f"   {i}. {result.title}")
            print(f"      Country: {result.country or 'N/A'}")
            print(f"      Total Fees: {result.total_fees or 'N/A'}")
            print(f"      URL: {result.url}")
            print()
    
    print(f"🎯 Process complete! Check {csv_file} for detailed results.")


if __name__ == "__main__":
    # ==========================================
    # CONFIGURATION - EDIT THESE VALUES
    # ==========================================
    
    # DATE RANGE SETTINGS
    START_DATE = "01 янв 2025"      # Change start date here
    END_DATE = "31 авг 2025"        # Change end date here
    
    # PARSER SETTINGS
    BATCH_SIZE = 20                # URLs per batch (smaller = more reliable)
    BATCH_PAUSE = 1.0              # Pause between batches (seconds)
    MAX_CONCURRENT = 10           # Max concurrent requests (lower = more reliable)
    MAX_RETRIES = 3                # Max retry attempts per URL
    
    # ==========================================
    # RUN WITH YOUR SETTINGS
    # ==========================================
    
    asyncio.run(main(
        start_date=START_DATE,
        end_date=END_DATE,
        batch_size=BATCH_SIZE,
        batch_pause=BATCH_PAUSE,
        max_concurrent=MAX_CONCURRENT,
        max_retries=MAX_RETRIES
    ))
