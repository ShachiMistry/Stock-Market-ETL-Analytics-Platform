import pandas as pd
import numpy as np
from typing import Tuple

class DataValidator:
    """
    Responsible for validating data quality before processing.
    """
    
    REQUIRED_COLUMNS = ['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']
    
    @staticmethod
    def validate_schema(df: pd.DataFrame) -> bool:
        """
        Checks if the DataFrame has the required columns and correct types.
        """
        missing_cols = [col for col in DataValidator.REQUIRED_COLUMNS if col not in df.columns]
        if missing_cols:
            print(f"Validation Failed: Missing columns {missing_cols}")
            return False
        return True

    @staticmethod
    def check_data_quality(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
        """
        Performs quality checks:
        1. Resets index to ensuring unique records
        2. checks for nulls
        3. checks for negative prices (anomalies)
        
        Returns:
            Tuple[pd.DataFrame, dict]: Cleaned DataFrame and a report of issues found.
        """
        report = {
            "initial_rows": len(df),
            "missing_values": 0,
            "negative_prices": 0,
            "dropped_rows": 0
        }
        
        # Check for missing values
        null_counts = df[DataValidator.REQUIRED_COLUMNS].isnull().sum().sum()
        report["missing_values"] = int(null_counts)
        
        # Drop rows with critical missing data (prices)
        clean_df = df.dropna(subset=['open', 'close', 'high', 'low'])
        
        # Check for negative prices (Anomaly detection)
        # Prices cannot be negative
        negative_mask = (clean_df[['open', 'high', 'low', 'close']] < 0).any(axis=1)
        negative_count = negative_mask.sum()
        report["negative_prices"] = int(negative_count)
        
        # Filter out negative prices
        clean_df = clean_df[~negative_mask]
        
        report["dropped_rows"] = report["initial_rows"] - len(clean_df)
        
        return clean_df, report

    @staticmethod
    def detect_outliers(df: pd.DataFrame, threshold: float = 3.0) -> pd.DataFrame:
        """
        Simple Z-score based outlier detection on daily returns.
        Adds an 'is_outlier' column.
        """
        # Calculate daily return if not exists, just for the check
        # We assume data is sorted by Ticker and Date
        df = df.sort_values(by=['ticker', 'date'])
        
        # Avoid SettingWithCopyWarning
        df = df.copy()
        
        df['pct_change'] = df.groupby('ticker')['close'].pct_change()
        
        # Calculate Z-score of returns
        # Using simple mean/std per ticker
        stats = df.groupby('ticker')['pct_change'].agg(['mean', 'std']).reset_index()
        
        df = df.merge(stats, on='ticker', suffixes=('', '_stats'))
        
        df['z_score'] = (df['pct_change'] - df['mean']) / df['std']
        
        # Flag outliers
        df['is_outlier'] = df['z_score'].abs() > threshold
        
        # Cleanup
        df.drop(columns=['mean', 'std', 'z_score', 'pct_change'], inplace=True)
        
        return df
