from spx_history import *

def get_adjusted_intraday_by_sym(symbol='GOOGL', all_div=None, hols=None, keep_unadj=False):
	# read all dividends data
	assert hols is not None, f"US holidays data missing"
	assert all_div is not None, f"dividend data is missing"
	
	# get intraday and split data
	ts = get_data_by_symbol_filename(symbol=symbol, filename='split')
	ti = get_data_by_symbol_filename(symbol=symbol, filename='intraday')

	# convert date to date type
	if len(ts) > 0:
		ts['date'] = pd.to_datetime(ts['date']).dt.date	

	# add datetime column from unix timestamp for raw unadjusted data
	ti = add_dt_us_intraday(ti)
	ti = ti.sort_values('datetime_us').reset_index(drop=True)
	ti['close'] = ti.groupby('date')['close'].ffill()

	# create date, time lattice
	start_date_sym	= ti['date'].min()
	end_date_sym	= ti['date'].max()
	start_time_sym	= dt.time(4, 0) # ti['time'].min()
	end_time_sym	= dt.time(19, 59) # ti['time'].max()

	# date and time lattice
	dates = pd.bdate_range(start=start_date_sym, end=end_date_sym, freq="D").date
	times = pd.date_range(
		start=dt.datetime.combine(dt.date.today(), start_time_sym),
		end=dt.datetime.combine(dt.date.today(), end_time_sym),
		freq="1min"
	).time

	# cartesian product
	lattice = pd.MultiIndex.from_product(
		[dates, times],
		names=["date", "time"]
	).to_frame(index=False)

	# do a left merge
	til = pd.merge(lattice, ti, on=['date', 'time'], how='left')

	# ffill close by date
	til['close'] = til.groupby('date')['close'].ffill()


	# keep close using prices in trading hours
	til_c = pd.DataFrame(til.groupby('date')['close'].last()).reset_index().sort_values('date').reset_index(drop=True)
	til_c['is_holiday'] = til_c['date'].apply(lambda x: x in hols)
	til_c['is_weekend'] = pd.to_datetime(til_c['date']).dt.weekday >= 5

	# drop weekends
	til_c = til_c[~til_c['is_weekend']].sort_values('date').reset_index(drop=True)
	til_c.drop(columns=['is_weekend'], inplace=True)

	# forward fill close
	til_c['close'] = til_c['close'].ffill()

	# parse split ratio
	if len(ts) > 0:
		ts["split_ratio"] = ts["split"].apply(lambda x: float(x.split("/")[1])/float(x.split("/")[0]))
		til_c = pd.merge(til_c, ts[['date', 'split_ratio']], on='date', how='left')
	else:
		til_c['split_ratio'] = np.nan

	# add split ratio and dividend data
	tdiv_sym = all_div[all_div['Code'] == symbol].rename({'Date' : 'date', 'Dividend' : 'div_usd'}, axis=1)[['date', 'div_usd']]
	if len(tdiv_sym) > 0:
		til_c = pd.merge(til_c, tdiv_sym, on='date', how='left')
	else:
		til_c['div_usd'] = np.nan

	# shift div and split adjustment prior to ex-date
	til_c['div_usd_pre_ex'] = til_c['div_usd'].shift(-1)
	til_c['div_adj'] = 1-(til_c['div_usd_pre_ex']/til_c['close'])
	til_c['split_adj'] = til_c['split_ratio'].shift(-1)

	# sort date
	til_c = til_c.sort_values('date', ascending=False).reset_index(drop=True)

	# cumulative split and div adjustments
	til_c['split_adj_cum'] = til_c['split_adj'].fillna(1).cumprod()
	til_c['div_adj_cum'] = til_c['div_adj'].fillna(1).cumprod()

	# cumulative adjustments
	til_c['adj_cum'] = til_c['split_adj_cum'] * til_c['div_adj_cum']

	# adjust close
	til_c['close_adj'] = til_c['close']*til_c['adj_cum']

	# add log and raw returns
	til_c["log_ret_1d"] = 100 * np.log(til_c["close_adj"]/til_c["close_adj"].shift(-1))
	til_c["ret_1d"] = 100 * (-1 + (til_c["close_adj"]/til_c["close_adj"].shift(-1)))

	# add adjustment factor to the lattice
	til = pd.merge(til, til_c[['date', 'split_adj_cum', 'div_adj_cum', 'adj_cum']], on='date', how='left')

	# combine date and time column
	til["datetime_us"] = pd.to_datetime(til["date"].astype(str) + " " + til["time"].astype(str))

	# filter useful columns
	til = til[['datetime_us', 'date', 'time', 'open', 'high', 'low', 'close', 'volume', 'split_adj_cum', 'div_adj_cum', 'adj_cum']]

	# adjust price and volume columns
	for c in ['open', 'close', 'low', 'high', 'volume']:
		til[f'raw_{c}'] = til[c]

		if c == 'volume':
			til[c] = (til[c] / til["adj_cum"]).round().fillna(0).astype("int64")
		else:
			til[c] = til[c] * til['adj_cum']

	if not keep_unadj:
		til.drop(columns=['raw_open', 'raw_close', 'raw_low', 'raw_high', 'raw_volume'], inplace=True)

	return til