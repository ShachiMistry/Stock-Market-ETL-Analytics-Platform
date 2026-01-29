import sys
import logging
import pandas as pd
from sqlalchemy import text
from src.config import Config
from src.utils.db import get_db_engine
from src.etl.ingestion import DataIngestion
from src.etl.validation import DataValidator
from src.etl.transformation import DataTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Stock Market ETL Pipeline...")
    
    # 1. Initialize Components
    try:
        engine = get_db_engine()
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful.")
    except Exception as e:
        logger.warning(f"Database connection failed: {e}. Proceeding without DB save (Dry Run).")
        engine = None

    # 2. Ingestion
    logger.info("--- Phase 1: Ingestion ---")
    tickers = Config.TICKERS
    ingestor = DataIngestion(tickers)
    
    # Fetch last 1 year of data
    raw_df = ingestor.fetch_data(period="1y")
    
    if raw_df.empty:
        logger.error("No data fetched. Exiting.")
        sys.exit(1)
        
    logger.info(f"Ingested {len(raw_df)} rows of raw data.")

    # 3. Validation
    logger.info("--- Phase 2: Validation ---")
    if not DataValidator.validate_schema(raw_df):
        logger.error("Schema validation failed.")
        sys.exit(1)
        
    clean_df, quality_report = DataValidator.check_data_quality(raw_df)
    logger.info(f"Data Quality Report: {quality_report}")
    
    # Detect pre-aggregation outliers
    clean_df = DataValidator.detect_outliers(clean_df)
    logger.info("Outlier detection complete.")

    # 4. Transformation
    logger.info("--- Phase 3: Transformation ---")
    enriched_df = DataTransformer.add_technical_indicators(clean_df)
    logger.info("Technical indicators added.")
    
    # Example aggregation (simulating a derived table)
    monthly_summary = DataTransformer.aggregate_metrics(enriched_df.copy(), freq='M')
    logger.info(f"Generated monthly summary: {len(monthly_summary)} records.")

    # 5. Load (Storage)
    logger.info("--- Phase 4: Loading ---")
    if engine:
        try:
            # We use 'replace' for the demo to allow re-runs. 
            # In validation/prod, we would use 'append' and handle duplicates carefully.
            enriched_df.to_sql('market_data_daily', engine, if_exists='replace', index=False)
            monthly_summary.to_sql('market_data_monthly', engine, if_exists='replace', index=True)
            logger.info("Data successfully saved to database.")
        except Exception as e:
            logger.error(f"Failed to write to database: {e}")
    else:
        logger.info("Skipping DB write (Dry Run Mode).")
        # For demo purposes, print head
        print("\n--- Sample Enriched Data ---")
        print(enriched_df[['date', 'ticker', 'close', 'sma_50', 'volatility_20d', 'is_outlier']].tail(10))

    logger.info("ETL Pipeline completed successfully.")

if __name__ == "__main__":
    main()
