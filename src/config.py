import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "stock_analytics")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
    
    # Construct Database URL
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # List of tickers to track
    TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
