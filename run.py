#!/usr/bin/env python3
"""Entry point for WeSense Home Assistant Ingester."""

import asyncio
from src.main import main

if __name__ == "__main__":
    asyncio.run(main())
