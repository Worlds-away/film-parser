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
    START_DATE = "11 сент 2025"      # Test start date
    END_DATE = "20 сент 2025"        # Test end date
    
    # PARSER SETTINGS FOR TEST
    BATCH_SIZE = 20                 # URLs per batch 
    BATCH_PAUSE = 1.0              # Pause between batches (seconds)
    MAX_CONCURRENT = 10             # Max concurrent requests
    MAX_RETRIES = 3                # Max retry attempts per URL
    
    # ==========================================
    
    print("Running with test configuration:")
    print(f"  Date range: {START_DATE} to {END_DATE}")
    print(f"  Parser: batch_size={BATCH_SIZE}, max_concurrent={MAX_CONCURRENT}")
    print()
    
    # Run the full workflow with test settings
    await run_film_discovery_and_parse(
        start_date=START_DATE,
        end_date=END_DATE,
        batch_size=BATCH_SIZE,
        batch_pause=BATCH_PAUSE,
        max_concurrent=MAX_CONCURRENT,
        max_retries=MAX_RETRIES
    )
    
    print("\n✅ Test completed!")


if __name__ == "__main__":
    asyncio.run(main())
