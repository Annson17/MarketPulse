"""
MarketPulse - PostgreSQL Database Initialization
================================================

Creates the database schema for storing historical market data.

Table: market_ticks
  - id: primary key
  - ticker: stock symbol (TCS, INFY, RELIANCE)
  - price: the quote price
  - seq_id: sequence number
  - timestamp: when the tick occurred
  
This persists historical data that dashboard loads on startup.
"""

import os
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Database connection string
# Update these credentials as needed
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "marketpulse")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create base class for ORM models
Base = declarative_base()

# ============================================================================
# DATABASE MODELS
# ============================================================================

class MarketTick(Base):
    """Historical market tick data"""
    __tablename__ = "market_ticks"
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False, index=True)
    price = Column(Float, nullable=False)
    seq_id = Column(Integer, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<MarketTick({self.ticker}, {self.price}, {self.seq_id}, {self.timestamp})>"


# ============================================================================
# INITIALIZATION
# ============================================================================

def init_db():
    """Create database and all tables"""
    print(f"Connecting to: {DATABASE_URL}")
    
    # Create engine
    engine = create_engine(DATABASE_URL)
    
    # Create all tables
    print("Creating tables...")
    Base.metadata.create_all(engine)
    print("✓ Database initialized successfully!")
    
    return engine


def get_session():
    """Get a new database session"""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()


if __name__ == "__main__":
    init_db()
