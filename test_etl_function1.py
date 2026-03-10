#!/usr/bin/env python
"""Test script for ETL function 1: fetch_items()"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from app.etl import fetch_items


async def test_fetch_items():
    """Test that fetch_items returns a list of dicts."""
    try:
        print("Testing fetch_items()...")
        items = await fetch_items()
        
        print(f"✓ fetch_items() returned {len(items)} items")
        
        # Validate structure
        if items:
            first = items[0]
            required_keys = {"lab", "task", "title", "type"}
            if required_keys.issubset(first.keys()):
                print(f"✓ First item has required keys: {list(first.keys())}")
                print(f"  Example: {first}")
            else:
                print(f"✗ First item missing keys. Has: {first.keys()}")
                return False
        
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_fetch_items())
    sys.exit(0 if success else 1)
