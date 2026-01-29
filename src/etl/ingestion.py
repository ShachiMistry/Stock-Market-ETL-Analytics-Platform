import yfinance as yf
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta

class DataIngestion:
    """
    Handles the ingestion of raw stock market data.
    """
    
    def __init__(self, tickers: List[str]):
        self.tickers = tickers

    def fetch_data(self, period: str = "1mo", interval: str = "1d") -> pd.DataFrame:
        """
        Fetches historical market data for the configured tickers.
        
        Args:
            period (str): The time period to download (e.g., '1d', '1mo', '1y').
            interval (str): The data interval (e.g., '1m', '1h', '1d').
            
        Returns:
            pd.DataFrame: A combined DataFrame containing data for all tickers.
        """
        print(f"Fetching data for {self.tickers} with period={period}, interval={interval}...")
        
        try:
            # yfinance download returns a multi-index dataframe if multiple tickers are passed
            data = yf.download(
                self.tickers, 
                period=period, 
                interval=interval, 
                group_by='ticker', 
                auto_adjust=True,
                prepost=True,
                threads=True
            )
            
            # Transformation to long format for easier processing
            # If multiple tickers, the columns are a MultiIndex (Ticker, Feature)
            # We want: Date, Ticker, Open, High, Low, Close, Volume
            
            if len(self.tickers) > 1:
                data = data.stack(level=0).reset_index()
                data.rename(columns={'level_1': 'Ticker'}, inplace=True)
            else:
                # If single ticker, just add the ticker column
                data['Ticker'] = self.tickers[0]
                data.reset_index(inplace=True)
                
            # Standardize column names
            data.columns = [col.lower() for col in data.columns]
            
            print(f"Successfully fetched {len(data)} records.")
            return data
            
        except Exception as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    # Test execution
    ingestor = DataIngestion(["AAPL", "GOOGL"])
    df = ingestor.fetch_data(period="5d")
    print(df.head())
