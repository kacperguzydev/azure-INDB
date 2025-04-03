import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_imdb_data(file_path):
    """
    Reads the IMDb Top 250 CSV file, performs basic cleaning,
    adds a unique 'id' field for Cosmos DB, and logs each step.

    :param file_path: Path to the CSV file.
    :return: A pandas DataFrame with processed data.
    """
    logger.info(f"Starting ETL process for file: {file_path}")

    try:
        df = pd.read_csv(file_path)
        logger.info("CSV loaded successfully.")
    except Exception as e:
        logger.error(f"Error reading CSV: {e}")
        raise e

    # Log initial record count
    initial_count = df.shape[0]
    logger.info(f"Initial record count: {initial_count}")

    # Drop rows with missing values
    df.dropna(inplace=True)
    after_drop_count = df.shape[0]
    logger.info(f"Dropped {initial_count - after_drop_count} rows with missing values. Remaining: {after_drop_count}")

    # Reset index and create unique 'id' column
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'id'}, inplace=True)
    df['id'] = df['id'].astype(str)
    logger.info("Unique 'id' field added.")

    # Convert 'year' column to integer if it exists
    if 'year' in df.columns:
        try:
            df['year'] = df['year'].astype(int)
            logger.info("Column 'year' converted to integer.")
        except Exception as e:
            logger.error(f"Error converting 'year': {e}")

    logger.info("ETL process completed successfully.")
    return df


if __name__ == '__main__':
    file_path = 'data/IMDB Top 250 Movies.csv'
    df_processed = process_imdb_data(file_path)
    logger.info("Processed Data (first 5 rows):")
    logger.info(df_processed.head().to_string())
