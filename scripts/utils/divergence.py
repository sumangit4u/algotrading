import datetime
import time
import pandas as pd
import numpy as np
import logging
from talib import RSI
from collections import deque
from scipy.signal import argrelextrema

global out_df
out_df = pd.DataFrame(columns=['Date1', 'Date2', 'Symbol', 'Signal', 'Type', 'RSI1', 'RSI2', 'Price1', 'Price2', 'Order'])

def getHigherLows(data: np.array, order=5, K=2):
    '''
    Finds consecutive higher lows in price pattern.
    Must not be exceeded within the number of periods indicated by the width
    parameter for the value to be confirmed.
    K determines how many consecutive lows need to be higher.
    '''
    # Get lows
    low_idx = argrelextrema(data, np.less, order=order)[0]
    lows = data[low_idx]
    # Ensure consecutive lows are higher than previous lows
    extrema = []
    ex_deque = deque(maxlen=K)
    for i, idx in enumerate(low_idx):
        if i == 0:
            ex_deque.append(idx)
            continue
        if lows[i] < lows[i - 1]:
            ex_deque.clear()

        ex_deque.append(idx)
        if len(ex_deque) == K:
            extrema.append(ex_deque.copy())

    return extrema


def getLowerHighs(data: np.array, order=5, K=2):
    '''
    Finds consecutive lower highs in price pattern.
    Must not be exceeded within the number of periods indicated by the width
    parameter for the value to be confirmed.
    K determines how many consecutive highs need to be lower.
    '''
    # Get highs
    high_idx = argrelextrema(data, np.greater, order=order)[0]
    highs = data[high_idx]
    # Ensure consecutive highs are lower than previous highs
    extrema = []
    ex_deque = deque(maxlen=K)
    for i, idx in enumerate(high_idx):
        if i == 0:
            ex_deque.append(idx)
            continue
        if highs[i] > highs[i - 1]:
            ex_deque.clear()

        ex_deque.append(idx)
        if len(ex_deque) == K:
            extrema.append(ex_deque.copy())

    return extrema


def getHigherHighs(data: np.array, order=5, K=2):
    '''
    Finds consecutive higher highs in price pattern.
    Must not be exceeded within the number of periods indicated by the width
    parameter for the value to be confirmed.
    K determines how many consecutive highs need to be higher.
    '''
    # Get highs
    high_idx = argrelextrema(data, np.greater, order=5)[0]
    highs = data[high_idx]
    # Ensure consecutive highs are higher than previous highs
    extrema = []
    ex_deque = deque(maxlen=K)
    for i, idx in enumerate(high_idx):
        if i == 0:
            ex_deque.append(idx)
            continue
        if highs[i] < highs[i - 1]:
            ex_deque.clear()

        ex_deque.append(idx)
        if len(ex_deque) == K:
            extrema.append(ex_deque.copy())

    return extrema


def getLowerLows(data: np.array, order=5, K=2):
    '''
    Finds consecutive lower lows in price pattern.
    Must not be exceeded within the number of periods indicated by the width
    parameter for the value to be confirmed.
    K determines how many consecutive lows need to be lower.
    '''
    # Get lows
    low_idx = argrelextrema(data, np.less, order=order)[0]
    lows = data[low_idx]
    # Ensure consecutive lows are lower than previous lows
    extrema = []
    ex_deque = deque(maxlen=K)
    for i, idx in enumerate(low_idx):
        if i == 0:
            ex_deque.append(idx)
            continue
        if lows[i] > lows[i - 1]:
            ex_deque.clear()

        ex_deque.append(idx)
        if len(ex_deque) == K:
            extrema.append(ex_deque.copy())

    return extrema


def check_divergence(df, order):
    higher_highs_close = getHigherHighs(df['Close'].values, order=order)
    lower_highs_close = getLowerHighs(df['Close'].values, order=order)
    higher_lows_close = getHigherLows(df['Close'].values, order=order)
    lower_lows_close = getLowerLows(df['Close'].values, order=order)

    if len(higher_highs_close) > 0:
        higher_highs_close = higher_highs_close[-1]
    if len(lower_highs_close) > 0:
        lower_highs_close = lower_highs_close[-1]
    if len(higher_lows_close) > 0:
        higher_lows_close = higher_lows_close[-1]
    if len(lower_lows_close) > 0:
        lower_lows_close = lower_lows_close[-1]

    higher_highs_rsi = getHigherHighs(df['RSI'].values, order=order)
    lower_highs_rsi = getLowerHighs(df['RSI'].values, order=order)
    higher_lows_rsi = getHigherLows(df['RSI'].values, order=order)
    lower_lows_rsi = getLowerLows(df['RSI'].values, order=order)

    if len(higher_highs_rsi) > 0:
        higher_highs_rsi = higher_highs_rsi[-1]
    if len(lower_highs_rsi) > 0:
        lower_highs_rsi = lower_highs_rsi[-1]
    if len(higher_lows_rsi) > 0:
        higher_lows_rsi = higher_lows_rsi[-1]
    if len(lower_lows_rsi) > 0:
        lower_lows_rsi = lower_lows_rsi[-1]

    # Regular Bearish
    # Prices are making higher highs but RSI is not.
    if len(higher_highs_close) > 0 and len(lower_highs_rsi) > 0 and higher_highs_close == lower_highs_rsi:
        # if df['DateTime'].iloc[higher_highs_close[1]]+datetime.timedelta(days=1) == datetime.date.today():
        symbol = df.iloc[higher_highs_close[0]]['Symbol']
        signal = 'Sell'
        type_ = 'Regular Bearish'
        price1 = df.iloc[higher_highs_close[0]]['Close']
        rsi1 = df.iloc[higher_highs_close[0]]['RSI']
        price2 = df.iloc[higher_highs_close[1]]['Close']
        rsi2 = df.iloc[higher_highs_close[1]]['RSI']
        datetime1 = df.iloc[higher_highs_close[0]]['DateTime']
        datetime2 = df.iloc[higher_highs_close[1]]['DateTime']
        out_df.loc[len(out_df.index)] = [datetime1, datetime2, symbol, signal, type_, rsi1, rsi2, price1, price2, order]
    #         print("************Regular Bearish Divergence found. Sell!!!**********")
    #         print('Order = ',order)
    #         print(df.iloc[higher_highs_close[0]])
    #         print(df.iloc[higher_highs_close[1]])

    # Hidden Bearish
    # Prices are making lower highs but RSI is not.
    if len(lower_highs_close) > 0 and len(higher_highs_rsi) and lower_highs_close == higher_highs_rsi:
        # if df['DateTime'].iloc[higher_lows_close[1]]+datetime.timedelta(days=1) == datetime.date.today():
        symbol = df.iloc[lower_highs_close[0]]['Symbol']
        signal = 'Sell'
        type_ = 'Hidden Bearish'
        price1 = df.iloc[lower_highs_close[0]]['Close']
        rsi1 = df.iloc[lower_highs_close[0]]['RSI']
        price2 = df.iloc[lower_highs_close[1]]['Close']
        rsi2 = df.iloc[lower_highs_close[1]]['RSI']
        datetime1 = df.iloc[lower_highs_close[0]]['DateTime']
        datetime2 = df.iloc[lower_highs_close[1]]['DateTime']
        out_df.loc[len(out_df.index)] = [datetime1, datetime2, symbol, signal, type_, rsi1, rsi2, price1, price2, order]
    #         print("************Hidden Bearish Divergence found. Sell!!!**********")
    #         print('Order = ',order)
    #         print(df.iloc[lower_highs_close[0]])
    #         print(df.iloc[lower_highs_close[1]])

    # Regular Bullish
    # Prices are making lower lows but RSI is not.
    if len(lower_lows_close) > 0 and len(higher_lows_rsi) > 0 and lower_lows_close == higher_lows_rsi:
        # if df['DateTime'].iloc[higher_highs_close[1]]+datetime.timedelta(days=1) == datetime.date.today():
        symbol = df.iloc[lower_lows_close[0]]['Symbol']
        signal = 'Buy'
        type_ = 'Regular Bullish'
        price1 = df.iloc[lower_lows_close[0]]['Close']
        rsi1 = df.iloc[lower_lows_close[0]]['RSI']
        price2 = df.iloc[lower_lows_close[1]]['Close']
        rsi2 = df.iloc[lower_lows_close[1]]['RSI']
        datetime1 = df.iloc[lower_lows_close[0]]['DateTime']
        datetime2 = df.iloc[lower_lows_close[1]]['DateTime']
        out_df.loc[len(out_df.index)] = [datetime1, datetime2, symbol, signal, type_, rsi1, rsi2, price1, price2, order]
    #         print("************Regular Bullish Divergence found. Buy!!!**********")
    #         print('Order = ',order)
    #         print(df.iloc[lower_lows_close[0]])
    #         print(df.iloc[lower_lows_close[1]])

    # Hidden Bullish
    # Prices are making higher lows but RSI is not.
    if len(higher_lows_close) > 0 and len(lower_lows_rsi) > 0 and higher_lows_close == lower_lows_rsi:
        # if df['DateTime'].iloc[higher_lows_close[1]]+datetime.timedelta(days=1) == datetime.date.today():
        symbol = df.iloc[higher_lows_close[0]]['Symbol']
        signal = 'Buy'
        type_ = 'Hidden Bullish'
        price1 = df.iloc[higher_lows_close[0]]['Close']
        rsi1 = df.iloc[higher_lows_close[0]]['RSI']
        price2 = df.iloc[higher_lows_close[1]]['Close']
        rsi2 = df.iloc[higher_lows_close[1]]['RSI']
        datetime1 = df.iloc[higher_lows_close[0]]['DateTime']
        datetime2 = df.iloc[higher_lows_close[1]]['DateTime']
        out_df.loc[len(out_df.index)] = [datetime1, datetime2, symbol, signal, type_, rsi1, rsi2, price1, price2, order]
    #         print("************Hidden Bullish Divergence found. Buy!!!**********")
    #         print('Order = ',order)
    #         print(df.iloc[higher_lows_close[0]])
    #         print(df.iloc[higher_lows_close[1]])
    return out_df

def calc_rsi(df,symbols):
    rsi_df = pd.DataFrame()
    for symbol in symbols:
        df_symbol = df.loc[df['Symbol']==symbol].copy()
        df_symbol['RSI'] = RSI(df_symbol['Close'].values)
        rsi_df = pd.concat([rsi_df,df_symbol],axis=0)
        #rsi_df['RSI'].fillna(0,inplace=True)
        rsi_df.dropna(inplace=True)
    return rsi_df