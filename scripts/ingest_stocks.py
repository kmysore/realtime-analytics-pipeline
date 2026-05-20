import os
import requests
import logging
from dotenv import load_dotenv
import snowflake.connector
import time

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TICKERS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'NVDA']


def fetch_stock_quote(symbol: str, api_key: str) -> dict:
    """
    Fetch a single stock quote from Alpha Vantage.
    Returns the raw Global Quote dict or raises on failure.
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": api_key,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    if "Global Quote" not in data or not data["Global Quote"]:
        raise ValueError(f"No data returned for {symbol}. Response: {data}")

    return data["Global Quote"]


def parse_quote(raw_quote: dict) -> dict:
    """
    Clean up Alpha Vantage's numbered keys into readable column names.
    Input:  {"01. symbol": "AAPL", "05. price": "190.75", ...}
    Output: {"symbol": "AAPL", "price": "190.75", ...}
    """
    return {
        "symbol":             raw_quote.get("01. symbol", ""),
        "open":               raw_quote.get("02. open", ""),
        "high":               raw_quote.get("03. high", ""),
        "low":                raw_quote.get("04. low", ""),
        "price":              raw_quote.get("05. price", ""),
        "volume":             raw_quote.get("06. volume", ""),
        "latest_trading_day": raw_quote.get("07. latest trading day", ""),
        "previous_close":     raw_quote.get("08. previous close", ""),
        "change":             raw_quote.get("09. change", ""),
        "change_percent":     raw_quote.get("10. change percent", ""),
    }


def load_to_snowflake(records: list[dict]) -> int:
    """
    Insert stock quote records into ANALYTICS.RAW.STOCK_PRICES_RAW.
    Returns the number of rows inserted.
    """
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
    )

    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO ANALYTICS.RAW.STOCK_PRICES_RAW (
            symbol, open, high, low, price, volume,
            latest_trading_day, previous_close, change, change_percent
        ) VALUES (
            %(symbol)s, %(open)s, %(high)s, %(low)s, %(price)s,
            %(volume)s, %(latest_trading_day)s, %(previous_close)s,
            %(change)s, %(change_percent)s
        )
    """

    cursor.executemany(insert_sql, records)
    conn.commit()

    rows_inserted = len(records)
    cursor.close()
    conn.close()

    return rows_inserted


def run_ingestion() -> None:
    """
    Main function — fetch all tickers and load to Snowflake.
    This is what the Airflow DAG will call.
    """
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY not set in environment")

    records = []

    for ticker in TICKERS:
        try:
            logger.info(f"Fetching quote for {ticker}...")
            raw_quote = fetch_stock_quote(ticker, api_key)
            parsed = parse_quote(raw_quote)
            records.append(parsed)
            logger.info(f"  {ticker}: ${parsed['price']}")
        except Exception as e:
            logger.error(f"  Failed to fetch {ticker}: {e}")
        
        time.sleep(1)

    if not records:
        raise ValueError("No records fetched — all tickers failed")

    logger.info(f"Loading {len(records)} records to Snowflake...")
    rows = load_to_snowflake(records)
    logger.info(f"Successfully loaded {rows} rows to RAW.STOCK_PRICES_RAW")


if __name__ == "__main__":
    run_ingestion()