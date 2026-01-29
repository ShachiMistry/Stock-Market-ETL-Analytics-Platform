import pytest
import pandas as pd
import numpy as np
from src.etl.validation import DataValidator
from src.etl.transformation import DataTransformer

@pytest.fixture
def sample_data():
    dates = pd.date_range(start='2023-01-01', periods=100)
    data = {
        'date': dates,
        'ticker': ['AAPL'] * 100,
        'open': np.random.rand(100) * 100 + 100,
        'high': np.random.rand(100) * 110 + 100,
        'low': np.random.rand(100) * 90 + 100,
        'close': np.random.rand(100) * 105 + 100,
        'volume': np.random.randint(1000, 10000, 100)
    }
    return pd.DataFrame(data)

def test_validation_schema(sample_data):
    assert DataValidator.validate_schema(sample_data) is True

def test_validation_missing_columns(sample_data):
    bad_df = sample_data.drop(columns=['close'])
    assert DataValidator.validate_schema(bad_df) is False

def test_quality_check_negative_prices(sample_data):
    # Introduce bad data
    sample_data.loc[0, 'close'] = -50
    clean_df, report = DataValidator.check_data_quality(sample_data)
    
    assert report['negative_prices'] == 1
    assert len(clean_df) == 99

def test_transformation_ma(sample_data):
    # Ensure sorted
    sample_data = sample_data.sort_values('date')
    enriched = DataTransformer.add_technical_indicators(sample_data)
    
    # Check if column exists
    assert 'sma_20' in enriched.columns
    
    # Check if logic holds (SMA calculation starts after window)
    assert pd.isna(enriched.iloc[18]['sma_20']) # 0-18 is 19 values
    assert not pd.isna(enriched.iloc[20]['sma_20'])

def test_outlier_detection(sample_data):
    # Force a massive outlier
    sample_data.loc[50, 'close'] = 10000 # returns will be huge
    
    outliers = DataValidator.detect_outliers(sample_data, threshold=3.0)
    
    # We expect some outliers now
    assert outliers['is_outlier'].sum() >= 1
