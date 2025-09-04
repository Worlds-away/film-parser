#!/usr/bin/env python3
"""
Test Script for Film Discovery and Parse

This script runs the full film_discovery_and_parse.py workflow with configurable options.
"""

import asyncio
from film_discovery_and_parse import main as run_film_discovery_and_parse


async def main():
    """Run the full film discovery and parse workflow with test configuration."""
    print("🎬 RUNNING PARSER")
    print("=" * 50)
    
    # ==========================================
    # TEST CONFIGURATION - EDIT THESE VALUES
    # ==========================================
    
    # DATE RANGE SETTINGS FOR TEST
    TEST_START_DATE = "20 авг 2025"      # Test start date
    TEST_END_DATE = "30 авг 2025"        # Test end date
    
    # PARSER SETTINGS FOR TEST
    TEST_BATCH_SIZE = 20                 # URLs per batch 
    TEST_BATCH_PAUSE = 1.0              # Pause between batches (seconds)
    TEST_MAX_CONCURRENT = 10             # Max concurrent requests
    TEST_MAX_RETRIES = 3                # Max retry attempts per URL
    
    # ==========================================
    
    print("Running with test configuration:")
    print(f"  Date range: {TEST_START_DATE} to {TEST_END_DATE}")
    print(f"  Parser: batch_size={TEST_BATCH_SIZE}, max_concurrent={TEST_MAX_CONCURRENT}")
    print()
    
    # Run the full workflow with test settings
    await run_film_discovery_and_parse(
        start_date=TEST_START_DATE,
        end_date=TEST_END_DATE,
        batch_size=TEST_BATCH_SIZE,
        batch_pause=TEST_BATCH_PAUSE,
        max_concurrent=TEST_MAX_CONCURRENT,
        max_retries=TEST_MAX_RETRIES
    )
    
    print("\n✅ Test completed!")


if __name__ == "__main__":
    asyncio.run(main())
