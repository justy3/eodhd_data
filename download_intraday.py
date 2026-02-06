from spx_history import *

# date range
start_dt = dt.date(2021,1,1)
end_dt = dt.date.today()

# tickers to query
tu = pd.read_csv('data/spy_cst.csv')
tickers = tu[tu['Ticker'] != '-']['Ticker'].unique().tolist()

# function to download
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

if __name__=='__main__':
	failed_tickers_intraday = download_data(get_raw_intraday, tickers, start_dt, end_dt)