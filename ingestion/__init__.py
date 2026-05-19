"""
MarketPulse Ingestion Module

This module contains the market data producer (market_maker.py) and consumer examples.
It's responsible for generating and distributing stock price ticks via Redis.
"""

from .market_maker import MarketMaker

__all__ = ["MarketMaker"]
