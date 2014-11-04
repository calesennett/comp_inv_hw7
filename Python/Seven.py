import QSTK.qstkutil.qsdateutil as du
import QSTK.qstkutil.tsutil as tsu
import QSTK.qstkutil.DataAccess as da
import QSTK.qstkstudy.EventProfiler as ep
import datetime as dt
import numpy as np
import copy
import pandas as pd
from itertools import *
import matplotlib.pyplot as plt

def main():
    s_date = dt.datetime(2008, 1, 1)
    e_date = dt.datetime(2010, 1, 1)
    lookback = 20
    print "Reading data..."
    data, symbols, timestamps = setup(s_date, e_date, lookback)
    indicator_bol = bol_band(data, symbols, timestamps, lookback, s_date)
    event_matrix = create_matrix(indicator_bol, data)
    ep.eventprofiler(event_matrix, data, i_lookback=20, i_lookforward=20, s_filename="SP500 BOL.pdf")

def create_matrix(data, prices):
    timestamps = data.index
    syms = data.columns
    events = copy.deepcopy(prices['close'])
    events = events * np.NAN

    for sym in syms:
        for i in range(1, len(timestamps)):
            today_val = data[sym][timestamps[i]]
            yester_val = data[sym][timestamps[i-1]]

            if (    today_val <= -2.0 \
                and yester_val >= -2.0 \
                and data['SPY'][timestamps[i]] >= 1.0):
                events[sym][timestamps[i]] = 1
    return events

def bol_band(data, syms, timestamps, lookback, s_date):
    indicator_bol_vals = pd.DataFrame(index=timestamps, columns=syms).truncate(before=s_date)
    prices = data['close']
    for sym in syms:
        rm = pd.rolling_mean(prices[sym], lookback).truncate(before=s_date)
        rstd = pd.rolling_std(prices[sym], lookback).truncate(before=s_date)
        for timestamp in rm.index:
            indicator_bol_vals[sym][timestamp] = (prices[sym][timestamp] - rm[timestamp]) / rstd[timestamp]

    return indicator_bol_vals

def setup(s_date, e_date, lookback):
    time_of_day = dt.timedelta(hours=16)
    timestamps = du.getNYSEdays(s_date - dt.timedelta(days=lookback+10), e_date, time_of_day)
    data, symbols = read_data(timestamps)
    return data, symbols, timestamps

def read_data(timestamps):
    data_obj = da.DataAccess('Yahoo')

    symbols = data_obj.get_symbols_from_list("sp5002012")
    symbols.append('SPY')
    keys = ['close']
    all_data = data_obj.get_data(timestamps, symbols, keys)

    # remove NaN from price data
    all_data = dict(zip(keys, all_data))
    for s_key in keys:
        all_data[s_key] = all_data[s_key].fillna(method='ffill')
        all_data[s_key] = all_data[s_key].fillna(method='bfill')
        all_data[s_key] = all_data[s_key].fillna(1.0)
    return all_data, symbols

if __name__ == "__main__":
    main()

