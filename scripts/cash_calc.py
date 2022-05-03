import pymongo
import pandas as pd
import numpy as np
import pandas.io.sql as psql
from scripts.utils.db_utils import store_dataframe_to_postgre

def calc_daily_indicators(df):
    df['dailympf'] = np.round(df['tottrdqty_lakhs'] / (df['totaltrades']+0.01) * 100000, 1)
    df['dailyvwap'] = np.round(df['tottrdval_lakhs'] / (df['tottrdqty_lakhs']+0.01), 1)

    return df

def populate_weekly_data(current_week,current_year,conn):
    #current_week_from_db = db.daily_equity.find({"week": int(current_week), "year": int(current_year)})
    # db.weekly_equity.delete_many({"week": int(current_week), "year": int(current_year)})
    current_week_from_db = psql.read_sql(f'select * from daily_equity where week = {current_week} and year = {current_year}',conn)
    cur = conn.cursor()
    cur.execute(f'delete from weekly_equity where week = {current_week} and year = {current_year}')
    conn.commit()
    cur.close()
    current_week_df = current_week_from_db.sort_values(by = ['symbol','timestamp'])
    current_week_df = current_week_df.groupby(['symbol','week','year']).agg({'high':'max','low':'min','open':'first','close':'last'}).reset_index()
    #db['weekly_equity'].insert_many(current_week_df.to_dict('records'))
    store_dataframe_to_postgre(current_week_df, 'weekly_equity', conn)

def populate_monthly_data(current_month,current_year,conn):
    # current_month_from_db = db.daily_equity.find({"month": int(current_month), "year": int(current_year)})
    # db.monthly_equity.delete_many({"month": int(current_month), "year": int(current_year)})
    current_month_from_db = psql.read_sql(f'select * from daily_equity where month = {current_month} and year = {current_year}', conn)
    cur = conn.cursor()
    cur.execute(f'delete from monthly_equity where month = {current_month} and year = {current_year}')
    conn.commit()
    cur.close()
    current_month_df = current_month_from_db.sort_values(by = ['symbol','timestamp'])
    current_month_df = current_month_df.groupby(['symbol','month','year']).agg({'high':'max','low':'min','open':'first','close':'last'}).reset_index()
    #db['monthly_equity'].insert_many(current_month_df.to_dict('records'))
    store_dataframe_to_postgre(current_month_df, 'monthly_equity', conn)

def calc_weekly_indicators(current_week,current_year,conn):
    # current_week_from_db = db.daily_equity.find({"week": int(current_week),"year":int(current_year)})
    # db.weekly_indicators.delete_many({"week": int(current_week),"year":int(current_year)})

    current_week_df = psql.read_sql(f'select * from daily_equity where week = {current_week} and year = {current_year}', conn)
    cur = conn.cursor()
    cur.execute(f'delete from weekly_indicators where week = {current_week} and year = {current_year}')
    conn.commit()
    cur.close()

    current_week_df = current_week_df.groupby('symbol')['tottrdqty_lakhs', 'tottrdval_lakhs', 'totaltrades'].agg('sum').reset_index()
    current_week_df['weeklympf'] = np.round(current_week_df['tottrdqty_lakhs'] / (current_week_df['totaltrades']+0.01) * 100000, 1)
    current_week_df['weeklyvwap'] = np.round(current_week_df['tottrdval_lakhs'] / (current_week_df['tottrdqty_lakhs']+0.01), 1)

    current_week_df['week'] = current_week
    current_week_df['year'] = current_year
    current_week_df = current_week_df[['symbol', 'week', 'year', 'weeklyvwap', 'weeklympf']]
    #db['weekly_indicators'].insert_many(current_week_df.to_dict('records'))
    store_dataframe_to_postgre(current_week_df, 'weekly_indicators', conn)


def calc_monthly_indicators(current_month,current_year,conn):
    # current_month_from_db = db.daily_equity.find({"month": int(current_month),"year":int(current_year)})
    # db.monthly_indicators.delete_many({"month": int(current_month),"year":int(current_year)})
    current_month_df = psql.read_sql(f'select * from daily_equity where month = {current_month} and year = {current_year}', conn)
    cur = conn.cursor()
    cur.execute(f'delete from monthly_indicators where month = {current_month} and year = {current_year}')
    conn.commit()
    cur.close()
    current_month_df = current_month_df.groupby('symbol')['tottrdqty_lakhs', 'tottrdval_lakhs', 'totaltrades'].agg('sum').reset_index()
    current_month_df['monthlympf'] = np.round(current_month_df['tottrdqty_lakhs'] / (current_month_df['totaltrades']+0.01) * 100000, 1)
    current_month_df['monthlyvwap'] = np.round(current_month_df['tottrdval_lakhs'] / (current_month_df['tottrdqty_lakhs']+0.01), 1)

    current_month_df['month'] = current_month
    current_month_df['year'] = current_year
    current_month_df = current_month_df[['symbol', 'month', 'year', 'monthlyvwap', 'monthlympf']]
    # db['monthly_indicators'].insert_many(monthly_indicator_df.to_dict('records'))
    store_dataframe_to_postgre(current_month_df, 'monthly_indicators', conn)