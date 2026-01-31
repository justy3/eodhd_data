import os
import io
import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

EODHD_API_TOKEN = "688892463a08d2.30221935"
BASE_URL = "https://eodhd.com/api"
SESSION = requests.Session()

def make_request(endpoint, params=None, api_token=EODHD_API_TOKEN):
	"""
	Helper to make a request with error handling.
	"""
	if params is None:
		params = {}
	else:
		params = params.copy()

	url = f"{BASE_URL}/{endpoint}"
	params['api_token'] = api_token
	params['fmt'] = 'json'
	
	try:
		response = SESSION.get(url, params=params, timeout=10)
		if response.status_code == 200:
			try:
				return response.json()
			except ValueError:
				# Sometimes CSV is returned or invalid JSON if error
				return response.text
		elif response.status_code == 429:
			print(f"Rate limit reached for {url}. Sleeping...")
			time.sleep(60) # Wait a minute
			return make_request(endpoint, params, api_token)
		else:
			print(f"Error {response.status_code} fetching {url}: {response.text[:200]}")
			return None
	except Exception as e:
		print(f"Exception fetching {url}: {e}")
		return None

def get_all_us_stocks(api_token=EODHD_API_TOKEN):
	"""
	Get list of all US stocks, including delisted ones.
	"""
	print("Fetching all US stocks (active + delisted)...")
	# delisted=1 includes delisted tickers
	data = make_request("exchange-symbol-list/US", {"delisted": 1}, api_token=api_token)
	
	if data and isinstance(data, list):
		df = pd.DataFrame(data)
		print(f"Retrieved {len(df)} US tickers.")
		return df
	else:
		print(f"Failed to retrieve stock list. Data type: {type(data)}")
		return pd.DataFrame()

def get_historical_constituents(index_symbol, date_from, date_to, api_token=EODHD_API_TOKEN):
	"""
	Get historical constituents for an index.
	
	Args:
		index_symbol (str): e.g., 'GSPC.INDX' for S&P 500, 'IXIC.INDX' for Nasdaq Composite.
		date_from (str): Start date 'YYYY-MM-DD'.
		date_to (str): End date 'YYYY-MM-DD'.
	"""
	print(f"Fetching historical constituents for {index_symbol}...")
	# historical=1 flag gives the historical components
	params = {
		"historical": 1,
		"from": date_from,
		"to": date_to
	}
	
	data = make_request(f"fundamentals/{index_symbol}", params, api_token=api_token)
	
	if not data:
		return None
		
	# Parse the 'HistoricalComponents' section
	hist_components = data.get("HistoricalComponents")
	if not hist_components:
		print(f"No historical components found for {index_symbol}. " 
			  "Note: Not all indices support historical constituents via this endpoint.")
		return None
		
	# Convert dictionary to DataFrame
	records = []
	for date_key, components in hist_components.items():
		if isinstance(components, dict):
			for _, comp_data in components.items():
				comp_data['Date'] = date_key
				records.append(comp_data)
		elif isinstance(components, list):
			for comp_data in components:
				comp_data['Date'] = date_key
				records.append(comp_data)
				
	df = pd.DataFrame(records)
	return df

def get_intraday_data_chunk(symbol, start_ts, end_ts, api_token=EODHD_API_TOKEN):
	"""
	Fetch a single chunk of intraday data.
	"""
	params = {
		"interval": "5m",
		"from": start_ts,
		"to": end_ts
	}
	data = make_request(f"intraday/{symbol}", params, api_token=api_token)
	return symbol, data

def get_stock_intraday_history(symbol, start_date_str, end_date_str, api_token=EODHD_API_TOKEN):
	"""
	Fetch 5-min OHLCV data for a specific stock over a long range.
	Handles splitting into 100-day chunks to respect API limits.
	"""
	# Parse dates and ensure we work with a consistent timezone (UTC usually for APIs)
	# EODHD expects Unix timestamps. If we use naive datetime, .timestamp() uses local time.
	
	start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
	end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
	
	all_data = []
	
	# Split into chunks of ~100 days
	chunk_days = 100
	current_start = start_dt
	
	chunks = []
	while current_start < end_dt:
		current_end = min(current_start + timedelta(days=chunk_days), end_dt)
		
		# API expects Unix timestamps
		from_ts = int(current_start.timestamp())
		to_ts = int(current_end.timestamp())
		
		chunks.append((from_ts, to_ts))
		
		# Next interval starts after current_end
		# EODHD 'to' is inclusive. To avoid overlap, we might need adjustments.
		# However, 5m bars: 00:00, 00:05...
		# If we fetch to 00:00, we get the candle at 00:00.
		# Next request should start at 00:05.
		current_start = current_end + timedelta(minutes=5)
		
	for from_ts, to_ts in chunks:
		_, data = get_intraday_data_chunk(symbol, from_ts, to_ts, api_token=api_token)
		
		if data and isinstance(data, list):
			all_data.extend(data)
		elif isinstance(data, dict):
			# API might return a dict like {"status": "Error", ...}
			print(f"API Error (dict) for {symbol}: {data}")
		elif isinstance(data, str):
			if "Valid API token" in data:
				print(f"API Token Error for {symbol}: {data}")
				break # Stop trying if token is bad
			else:
				print(f"Unknown API Response (str) for {symbol}: {data[:100]}...")
		else:
			# None or other
			pass

	return all_data

def fetch_data_parallel(ticker_list, start_date, end_date, max_workers=10, api_token=EODHD_API_TOKEN):
	"""
	Main function to fetch intraday data for a list of tickers in parallel.
	"""
	results = {}
	total_tickers = len(ticker_list)
	print(f"Starting parallel fetch for {total_tickers} tickers...")

	with ThreadPoolExecutor(max_workers=max_workers) as executor:
		# We map each ticker to a future task
		future_to_ticker = {
			executor.submit(get_stock_intraday_history, ticker, start_date, end_date, api_token): ticker
			for ticker in ticker_list
		}

		completed_count = 0
		for future in as_completed(future_to_ticker):
			ticker = future_to_ticker[future]
			try:
				data = future.result()
				results[ticker] = data
				completed_count += 1
				if completed_count % 10 == 0:
					print(f"Progress: {completed_count}/{total_tickers} done.")
			except Exception as exc:
				print(f"{ticker} generated an exception: {exc}")

	return results

if __name__ == "__main__":
	# Example usage when running the script directly
	print("--- Testing get_all_us_stocks ---")
	try:
		stocks = get_all_us_stocks()
		if not stocks.empty:
			print(stocks.head())
		else:
			print("No stocks returned.")
	except Exception as e:
		print(f"Error in get_all_us_stocks: {e}")

	print("\n--- Testing get_stock_intraday_history (AAPL.US) ---")
	# Fetch a small range just to test
	end_date = datetime.now().strftime("%Y-%m-%d")
	start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
	
	print(f"Fetching data from {start_date} to {end_date}...")
	try:
		data = get_stock_intraday_history("AAPL.US", start_date, end_date)
		
		if data:
			print(f"Retrieved {len(data)} records. First 3 records:")
			for row in data[:3]:
				print(row)
		else:
			print("No intraday data retrieved.")
	except Exception as e:
		print(f"Error in get_stock_intraday_history: {e}")