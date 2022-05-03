from talib import RSI,SMA,EMA, BBANDS
import talib
import pandas as pd
import numpy as np

def calc_rsi(df,period=14):
    col_name = 'rsi_' + str(period)
    df[col_name] = RSI(df['close'].values,timeperiod=period)
    return df

def calc_sma(df,period,on='close'):
    ts = df[on].values
    col_name = 'sma_'+str(period)+'_'+on
    df[col_name] = SMA(ts, timeperiod = period)
    return df

def calc_ema(df,period,on='close'):
    ts = df[on].values
    col_name = 'ema_'+str(period)+'_'+on
    df[col_name] = EMA(ts, timeperiod = period)
    return df

def calc_bband(df,wmd,type='s',dev=2.0,period = 20):
    if type == 's':
        mtype = talib.MA_Type.SMA
    if type == 'e':
        mtype = talib.MA_Type.EMA
    upper, middle, lower = BBANDS(df['close'], timeperiod=period,
                                       nbdevup=dev, nbdevdn=dev,
                                       matype=mtype)
    df['bband_upper_'+wmd] = upper
    df['bband_middle_'+wmd] = middle
    df['bband_lower_'+wmd] = lower
    return df

def calc_ndays_vwap(df,df_ind,days):
    assert len(df)==len(df_ind)
    col_name = str(days)+'days_vwap'
    sum_prod = df_ind['dailyvwap']*df['tottrdval_lakhs']
    rolling_sum = df['tottrdval_lakhs'].rolling(days).sum()
    df[col_name] = sum_prod.rolling(days).sum()/(rolling_sum+1)
    return df

def calc_narrow_range(df):
    ranges = df['high'] - df['low']


