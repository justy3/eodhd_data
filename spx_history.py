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
			response = requests.get(url, params=params, timeout=5)
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

def adjust_intraday_prices(intraday_df, splits_df, div_df):
	df = intraday_df.copy()

	# ensure datetime is timezone-aware
	df["datetime"] = pd.to_datetime(df["timestamp"], utc=True, unit="s")

	# convert to US market timezone
	df["datetime_us"] = df["datetime"].dt.tz_convert("America/New_York")

	# add date and time columns
	df["date"] = df["datetime_us"].dt.date
	df["time"] = df["datetime_us"].dt.time

	# adding split adjustment
	splits = splits_df.copy()
	splits["date"] = pd.to_datetime(splits["date"]).dt.date

	# parse split ratio (e.g. "10.000000/1.000000")
	splits["ratio"] = splits["split"].apply(
		lambda x: float(x.split("/")[0]) / float(x.split("/")[1])
	)

	# create cumulative split adjustment factor
	splits = splits.sort_values("date")
	splits["cum_split_factor"] = splits["ratio"][::-1].cumprod()[::-1]

	df = df.merge(
		splits[["date", "cum_split_factor"]],
		on="date",
		how="left"
	)

	df["cum_split_factor"] = df["cum_split_factor"].ffill().fillna(1.0)

	# apply split adjustment
	price_cols = ["open", "high", "low", "close"]
	for c in price_cols:
		df[c] = df[c] / df["cum_split_factor"]

	df["volume"] = df["volume"] * df["cum_split_factor"]

	# dividend adjustment
	divs = div_df.copy()
	divs["date"] = pd.to_datetime(divs["recordDate"]).dt.date
	divs = divs.sort_values("date")

	divs["cum_div"] = divs["unadjustedValue"][::-1].cumsum()[::-1]

	df = df.merge(
		divs[["date", "cum_div"]],
		on="date",
		how="left"
	)

	df["cum_div"] = df["cum_div"].ffill().fillna(0.0)

	for c in price_cols:
		df[c] = df[c] - df["cum_div"]

	# cleanup
	# df = df.drop(columns=["cum_split_factor", "cum_div"])

	return df

def download_data(fun_to_d, tickers, start_dt, end_dt):
	fun_to_name = {
		get_splits : "split",
		get_dividends : "div",
		get_raw_intraday : "intraday"
	}

	# dataset name
	data_name = fun_to_name[fun_to_d]

	# create directory if doesnt exist
	csv_path_dir = f"data/{data_name}/"
	Path(csv_path_dir).mkdir(parents=True, exist_ok=True)

	# failed tickers
	failed_tickers = {}

	for sym in tickers:
		csv_file_path = f"{csv_path_dir}/{sym}.csv"

		if Path(csv_file_path).is_file():
			qt.log.info(f"csv file for {sym} already exists. skip")
			continue
	
		else:
			try:
				qt.log.info(f"querying {data_name} data for symbol {sym}")
				t = fun_to_d(sym, start_dt, end_dt)
				if len(t) != 0:
					qt.log.info(f"saving {t.shape} rows for symbol {sym} to file {csv_file_path}")
					t.to_csv(csv_file_path, index=False)

				# 
				else:
					assert False, "0 rows returned"

				# delete table variable
				del t

			except Exception as e:
				qt.log.warning(f"error occurred with ticker : {sym}, error : {e}")
				failed_tickers[sym] = e
	
	return failed_tickers