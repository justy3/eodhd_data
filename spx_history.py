import os
import qt
import time
import requests

from qt import dt, np, pd, os
from pathlib import Path

EODHD_API_KEY = os.environ.get('EODHD_API_KEY')

def get_splits(symbol, start_date, end_date, api_token=EODHD_API_KEY):
	"""Fetches split events. Format: 2024-06-07, 10.0 (for a 10-for-1 split)"""
	url = f"https://eodhd.com/api/splits/{symbol}"
	params = {
		"api_token": api_token,
		"fmt": "json",
		"from": start_date,
		"to": end_date
	}
	try:
		qt.log.info(f"querying split data for symbol {symbol}")
		response = requests.get(url, params=params)
		ts = pd.DataFrame(response.json())
		qt.log.info(f"returned {ts.shape} rows")
		return ts

	except Exception as e:
		qt.log.warning(f"couldnt query split data for stock {symbol}, error : {e}")
		return pd.DataFrame()

def get_dividends(symbol, start_date, end_date, api_token=EODHD_API_KEY):
	"""Fetches dividend history with ex-dates and values."""
	url = f"https://eodhd.com/api/div/{symbol}"
	params = {
		"api_token": api_token,
		"fmt": "json",
		"from": start_date,
		"to": end_date
	}
	try:
		qt.log.info(f"querying dividend data for symbol {symbol}")
		response = requests.get(url, params=params)
		td = pd.DataFrame(response.json())
		qt.log.info(f"returned {td.shape} rows")
		return td

	except Exception as e:
		qt.log.warning(f"couldnt query dividend data for stock {symbol}, error : {e}")
		return pd.DataFrame()

def date_to_unix(date):
	return int(pd.to_datetime(date).replace(tzinfo=dt.timezone.utc).timestamp())

def get_raw_intraday(symbol, start_dt, end_dt, api_token=EODHD_API_KEY):
	"""Downloads raw 1m intraday data, handling the 120-day limit."""
	all_data = []
	
	try:

		current_start = start_dt
		while current_start < end_dt:
			# 100-day chunks for safety
			current_end = min(current_start + dt.timedelta(days=100), end_dt)
			
			url = f"https://eodhd.com/api/intraday/{symbol}"
			params = {
				"api_token": api_token,
				"interval": "1m",
				"fmt": "json",
				"from": date_to_unix(current_start),
				"to": date_to_unix(current_end)
			}
			
			qt.log.info(f"querying intraday data for symbol {symbol} from {current_start} to {current_end}")
			response = requests.get(url, params=params)
			if response.status_code == 200:
				all_data.extend(response.json())
			
			current_start = current_end + dt.timedelta(seconds=1)
			time.sleep(0.2) # To respect rate limits

		df = pd.DataFrame(all_data)
		if not df.empty:
			df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
			df.set_index('datetime', inplace=True)
			df.rename(columns={'o':'open','h':'high','l':'low','c':'close','v':'volume'}, inplace=True)
	
	except Exception as e:
		qt.log.warning(f"couldnt get raw intraday data for stock {symbol}, error : {e}")
		df = pd.DataFrame()

	return df