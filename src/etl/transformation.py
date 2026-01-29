import pandas as pd
import numpy as np

class DataTransformer:
    """
    Handles the transformation and feature engineering of stock data.
    """
    
    @staticmethod
    def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds technical indicators: Returns, MA, Volatility.
        Expects a clean DataFrame with columns: date, ticker, close
        """
        # Ensure we are sorted
        df = df.sort_values(by=['ticker', 'date'])
        
        # We process by group (Ticker)
        grouped = df.groupby('ticker')
        
        # 1. Daily Returns
        # We use .transform to keep the original index and shape
        df['daily_return'] = grouped['close'].pct_change()
        
        # 2. Log Returns (better for additive properties)
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))
        
        # 3. Rolling Averages (SMA)
        df['sma_20'] = grouped['close'].transform(lambda x: x.rolling(window=20).mean())
        df['sma_50'] = grouped['close'].transform(lambda x: x.rolling(window=50).mean())
        
        # 4. Volatility (Standard Deviation of returns over a window)
        # Annualized volatility approx: std_dev * sqrt(252)
        df['volatility_20d'] = grouped['daily_return'].transform(lambda x: x.rolling(window=20).std()) * np.sqrt(252)
        
        # 5. Momentum (e.g., RSI is complex, simple momentum is price diff)
        df['momentum_10d'] = df['close'] - grouped['close'].transform(lambda x: x.shift(10))
        
        # Fill NaN values created by rolling windows (optional, or leave as null)
        # For a clean DB insert, strictly we might drop the initial rows or fill with 0. 
        # Here we'll drop rows that don't have the longest window (50) to ensure high quality training data
        # BUT for general analytics, keeping them is fine. Let's just fill NaNs with 0 for now or leave them.
        # Leaving them allows the DB to handle it or downstream to handle it.
        
        return df

    @staticmethod
    def aggregate_metrics(df: pd.DataFrame, freq: str = 'M') -> pd.DataFrame:
        """
        Aggregates data to a lower frequency (e.g., Monthly)
        
        Args:
            df: Daily data
            freq: 'W' (Weekly), 'M' (Monthly)
        """
        df.set_index('date', inplace=True)
        
        # Custom aggregation
        agg_rules = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'daily_return': lambda x: (1 + x).prod() - 1  # Compounding returns
        }
        
        aggregated = df.groupby(['ticker']).resample(freq).agg(agg_rules).reset_index()
        
        return aggregated
